#pragma once

#include "public.h"

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/rpc/channel.h>

#include <yt/yt/client/election/public.h>

#include <mutex>

namespace NYT::NHydraStressTest {

////////////////////////////////////////////////////////////////////////////////

//! Deterministic fault injected into requests of a single RPC method.
DEFINE_ENUM(EMethodFault,
    (None)
    // Fail every request immediately with a transport error.
    (Fail)
    // Keep requests until #TPeerChannel::ReleaseHeld is called.
    (Hold)
    // Let requests through one at a time until one of them succeeds, then fail all subsequent ones.
    (FailAfterFirstSuccess)
);

DEFINE_ENUM(EFirstSuccessState,
    (Idle)
    (InFlight)
    (Succeeded)
);

class TPeerChannel
    : public NRpc::IChannel
{
public:
    const std::string& GetEndpointDescription() const override;

    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override;

    NRpc::IClientRequestControlPtr Send(
        NRpc::IClientRequestPtr request,
        NRpc::IClientResponseHandlerPtr responseHandler,
        const NRpc::TSendOptions& options) override;

    void Terminate(const TError& error) override;

    void SubscribeTerminated(const TCallback<void(const TError&)>& callback) override;

    void UnsubscribeTerminated(const TCallback<void(const TError&)>& callback) override;

    void SetUnderlying(NRpc::IChannelPtr underlying);

    void SetBroken(bool broken);

    //! Injects #fault into requests of #method sent through this (directed) channel.
    //! Setting a new fault resets the "first success" state; replacing #EMethodFault::Hold
    //! sends the held requests in order before any request sent after them.
    void SetMethodFault(const std::string& method, EMethodFault fault);
    //! Sends all requests held by #EMethodFault::Hold and switches the fault off.
    void ReleaseHeld();
    //! For #EMethodFault::FailAfterFirstSuccess: true once a request has succeeded.
    bool HasSucceededOnce() const;

    int GetInflightRequestCount() override;

    const IMemoryUsageTrackerPtr& GetChannelMemoryTracker() override;

private:
    NRpc::IChannelPtr Underlying_;
    std::atomic<bool> Broken_ = false;

    mutable std::mutex FaultLock_;
    std::string FaultMethod_;
    EMethodFault Fault_ = EMethodFault::None;
    std::vector<TClosure> HeldRequests_;
    // NB: Replaced on every fault change so that late responses do not affect the new fault.
    // Atomic since response handlers update it without FaultLock_.
    std::shared_ptr<std::atomic<EFirstSuccessState>> FirstSuccessState_ =
        std::make_shared<std::atomic<EFirstSuccessState>>(EFirstSuccessState::Idle);
};

DEFINE_REFCOUNTED_TYPE(TPeerChannel)

////////////////////////////////////////////////////////////////////////////////

class TChannelManager
    : public TRefCounted
{
public:
    explicit TChannelManager(int peerCount);

    NRpc::TStaticChannelFactoryPtr GetChannelFactory(int peerId);

    void SetUnderlying(NRpc::IChannelPtr channel, int peerId);

    void SetBroken(int from, int to, bool broken);

    //! Returns the directed channel used by peer #from to talk to peer #to.
    TPeerChannelPtr GetChannel(int from, int to);

private:
    std::vector<std::vector<TPeerChannelPtr>> Channels_;
    std::vector<NRpc::TStaticChannelFactoryPtr> ChannelFactories_;
};

DEFINE_REFCOUNTED_TYPE(TChannelManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest
