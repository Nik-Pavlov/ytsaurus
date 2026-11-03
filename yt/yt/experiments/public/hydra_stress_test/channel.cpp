#include "channel.h"
#include "helpers.h"

#include <yt/yt/core/rpc/client.h>
#include <yt/yt/core/rpc/static_channel_factory.h>
#include <yt/yt/core/rpc/channel_detail.h>

#include <util/datetime/base.h>

namespace NYT::NHydraStressTest {

using namespace NConcurrency;
using namespace NElection;
using namespace NRpc;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = HydraStressTestLogger;

////////////////////////////////////////////////////////////////////////////////

const std::string& TPeerChannel::GetEndpointDescription() const
{
    return Underlying_->GetEndpointDescription();
}

const IAttributeDictionary& TPeerChannel::GetEndpointAttributes() const
{
    return Underlying_->GetEndpointAttributes();
}

namespace {

class TFirstSuccessTrackingResponseHandler
    : public IClientResponseHandler
{
public:
    TFirstSuccessTrackingResponseHandler(
        IClientResponseHandlerPtr underlying,
        std::shared_ptr<std::atomic<EFirstSuccessState>> state)
        : Underlying_(std::move(underlying))
        , State_(std::move(state))
    { }

    void HandleAcknowledgement() override
    {
        Underlying_->HandleAcknowledgement();
    }

    void HandleResponse(
        TSharedRefArray message,
        const std::string& address,
        NBus::IDirectPlacementTransferPtr attachmentsTransfer) override
    {
        State_->store(EFirstSuccessState::Succeeded);
        Underlying_->HandleResponse(std::move(message), address, std::move(attachmentsTransfer));
    }

    void HandleError(TError error, const std::string& address) override
    {
        // Free the slot: pings to a peer that is still starting fail, and they must not use up the first success.
        auto expected = EFirstSuccessState::InFlight;
        State_->compare_exchange_strong(expected, EFirstSuccessState::Idle);
        Underlying_->HandleError(std::move(error), address);
    }

    void HandleStreamingPayload(const TStreamingPayload& payload) override
    {
        Underlying_->HandleStreamingPayload(payload);
    }

    void HandleStreamingFeedback(const TStreamingFeedback& feedback) override
    {
        Underlying_->HandleStreamingFeedback(feedback);
    }

private:
    const IClientResponseHandlerPtr Underlying_;
    const std::shared_ptr<std::atomic<EFirstSuccessState>> State_;
};

} // namespace

IClientRequestControlPtr TPeerChannel::Send(
    IClientRequestPtr request,
    IClientResponseHandlerPtr responseHandler,
    const TSendOptions& options)
{
    auto error = TError(NRpc::EErrorCode::TransportError, "Channel is broken");

    {
        std::unique_lock guard(FaultLock_);
        if (Fault_ != EMethodFault::None && request->GetMethod() == FaultMethod_) {
            switch (Fault_) {
                case EMethodFault::Fail:
                    guard.unlock();
                    responseHandler->HandleError(TError(
                        NRpc::EErrorCode::TransportError,
                        "Injected %v failure",
                        FaultMethod_));
                    return nullptr;

                case EMethodFault::Hold: {
                    auto thunk = New<TClientRequestControlThunk>();
                    HeldRequests_.push_back(BIND([=, this, this_ = MakeStrong(this)] {
                        thunk->SetUnderlying(Underlying_->Send(request, responseHandler, options));
                    }));
                    return thunk;
                }

                case EMethodFault::FailAfterFirstSuccess: {
                    // Claim the request slot now: the success is only known when the response arrives.
                    auto state = EFirstSuccessState::Idle;
                    if (!FirstSuccessState_->compare_exchange_strong(state, EFirstSuccessState::InFlight)) {
                        guard.unlock();
                        responseHandler->HandleError(TError(
                            NRpc::EErrorCode::TransportError,
                            "Injected %v failure (FirstSuccessState: %v)",
                            FaultMethod_,
                            state));
                        return nullptr;
                    }
                    responseHandler = New<TFirstSuccessTrackingResponseHandler>(
                        std::move(responseHandler),
                        FirstSuccessState_);
                    break;
                }

                default:
                    YT_ABORT();
            }
        }
    }

    if (Broken_) {
        auto errorKind = rand() % 3;
        if (errorKind == 0) {
            auto timeout = FromProto<TDuration>(request->Header().timeout());
            TDelayedExecutor::Submit(
                BIND([responseHandler = std::move(responseHandler), error = std::move(error)] () mutable {
                    responseHandler->HandleError(std::move(error));
                }),
                timeout);
            YT_TLOG_DEBUG("Request timeout");
            return nullptr;
        }

        if (errorKind == 1) {
            responseHandler->HandleError(std::move(error));
            return nullptr;
        }
    }

    auto thunk = New<TClientRequestControlThunk>();
    TDelayedExecutor::Submit(
        BIND([=, this, this_ = MakeStrong(this)] {
            auto requestControl = Underlying_->Send(request, responseHandler, options);
            thunk->SetUnderlying(requestControl);
        }),
        TDuration::MilliSeconds(rand() % 100));

    if (Broken_) {
        return nullptr;
    }
    return thunk;
}

void TPeerChannel::Terminate(const TError& error)
{
    Underlying_->Terminate(error);
}

void TPeerChannel::SubscribeTerminated(const TCallback<void(const TError&)>& callback)
{
    Underlying_->SubscribeTerminated(callback);
}

void TPeerChannel::UnsubscribeTerminated(const TCallback<void(const TError&)>& callback)
{
    Underlying_->UnsubscribeTerminated(callback);
}

void TPeerChannel::SetUnderlying(IChannelPtr underlying)
{
    Underlying_ = underlying;
}

void TPeerChannel::SetBroken(bool broken)
{
    Broken_.store(broken);
}

void TPeerChannel::SetMethodFault(const std::string& method, EMethodFault fault)
{
    // Requests that arrive while the held ones are being sent are held as well,
    // so that a stale held request cannot overtake a newer one.
    while (true) {
        std::vector<TClosure> heldRequests;
        {
            std::lock_guard guard(FaultLock_);
            if (HeldRequests_.empty()) {
                FaultMethod_ = method;
                Fault_ = fault;
                FirstSuccessState_ = std::make_shared<std::atomic<EFirstSuccessState>>(EFirstSuccessState::Idle);
                return;
            }
            heldRequests = std::exchange(HeldRequests_, {});
        }
        for (const auto& send : heldRequests) {
            send();
        }
    }
}

void TPeerChannel::ReleaseHeld()
{
    SetMethodFault({}, EMethodFault::None);
}

bool TPeerChannel::HasSucceededOnce() const
{
    std::lock_guard guard(FaultLock_);
    return FirstSuccessState_->load() == EFirstSuccessState::Succeeded;
}

int TPeerChannel::GetInflightRequestCount()
{
    return Underlying_->GetInflightRequestCount();
}

const IMemoryUsageTrackerPtr& TPeerChannel::GetChannelMemoryTracker()
{
    return Underlying_->GetChannelMemoryTracker();
}

////////////////////////////////////////////////////////////////////////////////

TChannelManager::TChannelManager(int peerCount)
{
    Channels_.resize(peerCount);

    for (int from = 0; from < peerCount; ++from) {
        Channels_[from].resize(peerCount);
        ChannelFactories_.push_back(New<TStaticChannelFactory>());
        for (int to = 0; to < peerCount; ++to) {
            auto channel = New<TPeerChannel>();
            Channels_[from][to] = channel;
            ChannelFactories_[from]->Add(GetPeerAddress(to), channel);
        }
    }
}

TStaticChannelFactoryPtr TChannelManager::GetChannelFactory(int peerId)
{
    return ChannelFactories_[peerId];
}

void TChannelManager::SetUnderlying(IChannelPtr channel, int peerId)
{
    for (auto& channels : Channels_) {
        channels[peerId]->SetUnderlying(channel);
    }
}

void TChannelManager::SetBroken(int from, int to, bool broken)
{
    Channels_[from][to]->SetBroken(broken);
    Channels_[to][from]->SetBroken(broken);
}

TPeerChannelPtr TChannelManager::GetChannel(int from, int to)
{
    return Channels_[from][to];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest
