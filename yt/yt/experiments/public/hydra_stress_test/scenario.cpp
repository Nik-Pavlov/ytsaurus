#include "scenario.h"
#include "channel.h"
#include "checkers.h"
#include "config.h"
#include "helpers.h"
#include "peer.h"
#include "peer_proxy.h"

#include <yt/yt/ytlib/election/cell_manager.h>
#include <yt/yt/ytlib/election/config.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/rpc/static_channel_factory.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NHydraStressTest {

using namespace NConcurrency;
using namespace NElection;
using namespace NHydra;
using namespace NRpc;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = HydraStressTestLogger;

static const std::string AcceptMutationsMethod = "AcceptMutations";

////////////////////////////////////////////////////////////////////////////////

namespace {

constexpr int PeerCount = 5;

TConfigPtr MakeScenarioConfig(const TString& workDir)
{
    auto yson = Format(
        "{"
        "  peer_count = %v;"
        "  voting_peer_count = %v;"
        "  client_count = 0;"
        "  changelogs = { path = \"%v/changelogs\" };"
        "  snapshots = { path = \"%v/snapshots\" };"
        "}",
        PeerCount,
        PeerCount,
        workDir,
        workDir);
    return ConvertTo<TConfigPtr>(TYsonString(yson));
}

class TScenario
{
public:
    explicit TScenario(const TString& workDir)
        : Config_(MakeScenarioConfig(workDir))
        , ChannelManager_(New<TChannelManager>(PeerCount))
        , SnapshotIOQueue_(New<TActionQueue>("SnapshotIO"))
    {
        auto cellConfig = New<TCellConfig>();
        for (int i = 0; i < PeerCount; ++i) {
            auto peerConfig = New<TCellPeerConfig>();
            peerConfig->Address = GetPeerAddress(i);
            peerConfig->Voting = true;
            cellConfig->Peers.push_back(peerConfig);
        }

        auto linearizabilityChecker = New<TLinearizabilityChecker>(PeerCount);
        for (int i = 0; i < PeerCount; ++i) {
            auto peer = New<TPeer>(
                Config_,
                cellConfig,
                ChannelManager_->GetChannelFactory(i),
                i,
                SnapshotIOQueue_->GetInvoker(),
                linearizabilityChecker,
                /*voting*/ true);
            ChannelManager_->SetUnderlying(peer->GetChannel(), i);
            Peers_.push_back(peer);
        }
    }

    int Run()
    {
        for (const auto& peer : Peers_) {
            peer->Initialize();
        }

        int leaderId = WaitForLeader({0, 1, 2, 3, 4});
        std::vector<int> followerIds;
        for (int i = 0; i < PeerCount; ++i) {
            if (i != leaderId) {
                followerIds.push_back(i);
            }
        }
        int a = followerIds[0];
        int b = followerIds[1];
        int c = followerIds[2];
        int d = followerIds[3];
        YT_TLOG_INFO("Scenario roles assigned")
            .With("Leader", leaderId)
            .With("A", a)
            .With("B", b)
            .With("C", c)
            .With("D", d);

        WaitForCondition(
            [&] { return std::all_of(followerIds.begin(), followerIds.end(), [&] (int id) { return Peers_[id]->IsActive(); }); },
            "active followers");

        YT_VERIFY(WaitFor(SendCas(leaderId, 0, 1)).ValueOrThrow()->success());
        WaitForCondition(
            [&] {
                auto leaderVersion = Peers_[leaderId]->GetLastNonemptyLoggedVersion();
                return std::all_of(followerIds.begin(), followerIds.end(), [&] (int id) {
                    return Peers_[id]->GetLastNonemptyLoggedVersion() == leaderVersion;
                });
            },
            "followers to log the baseline");

        // 1. C and D stop logging, B's AcceptMutations are held.
        Channel(leaderId, c)->SetMethodFault(AcceptMutationsMethod, EMethodFault::Fail);
        Channel(leaderId, d)->SetMethodFault(AcceptMutationsMethod, EMethodFault::Fail);
        Channel(leaderId, b)->SetMethodFault(AcceptMutationsMethod, EMethodFault::Hold);
        DrainChannels();
        auto baselineVersion = Peers_[a]->GetLastNonemptyLoggedVersion();
        YT_TLOG_INFO("Baseline logged")
            .With("Version", baselineVersion);

        // 2. A mutation that is logged by the leader and A only.
        auto asyncCas = SendCas(leaderId, 1, 2);
        if (!TryWaitForCondition(
            [&] { return Peers_[a]->GetLastNonemptyLoggedVersion() > baselineVersion; },
            "A to log the mutation"))
        {
            return 2;
        }
        YT_TLOG_INFO("Mutation logged by the leader and A")
            .With("Version", Peers_[a]->GetLastNonemptyLoggedVersion());
        // Let A acknowledge the mutation to the leader before it restarts.
        DrainChannels();
        if (asyncCas.IsSet()) {
            YT_TLOG_ERROR("CAS completed before B logged it; scenario is inconclusive");
            return 2;
        }

        // 3. A restarts and rejoins the same epoch. It may receive exactly one AcceptMutations
        // (the initial ping that starts its recovery) and nothing after it, so that it cannot
        // re-log the mutation from the leader.
        YT_TLOG_INFO("Restarting follower A")
            .With("PeerId", a);
        WaitFor(Peers_[a]->Finalize())
            .ThrowOnError();
        // NB: The fault is armed only now, so that the old instance cannot consume the first success.
        Channel(leaderId, a)->SetMethodFault(AcceptMutationsMethod, EMethodFault::FailAfterFirstSuccess);
        DrainChannels();
        Peers_[a]->Initialize();
        WaitForCondition([&] { return Channel(leaderId, a)->HasSucceededOnce(); }, "A to receive the initial ping");
        if (!TryWaitForCondition(
            [&] { return Peers_[a]->GetLastNonemptyLoggedVersion() <= baselineVersion; },
            "A to truncate the mutation during recovery"))
        {
            return 2;
        }
        // NB: A does not become active: it cannot finish catching up without further AcceptMutations.
        YT_TLOG_INFO("A dropped the mutation during recovery")
            .With("Version", Peers_[a]->GetLastNonemptyLoggedVersion());

        // 4. B logs the mutation, the leader commits and acknowledges it.
        Channel(leaderId, b)->ReleaseHeld();
        auto rspOrError = WaitFor(asyncCas.WithTimeout(TDuration::Seconds(30)));
        if (!rspOrError.IsOK() || !rspOrError.Value()->success()) {
            YT_TLOG_ERROR("CAS was not acknowledged; scenario is inconclusive")
                .With(rspOrError);
            return 2;
        }
        TValue expectedValue = 2;
        YT_TLOG_INFO("Write acknowledged to the client")
            .With("Value", expectedValue);

        if (auto version = Peers_[a]->GetLastNonemptyLoggedVersion(); version > baselineVersion) {
            YT_TLOG_ERROR("A logged the mutation again; scenario is inconclusive")
                .With("Version", version);
            return 2;
        }

        // 5. Two failures out of five: the leader and B.
        YT_TLOG_INFO("Stopping the leader and B")
            .With("LeaderId", leaderId)
            .With("B", b);
        auto stopLeader = Peers_[leaderId]->Finalize();
        auto stopB = Peers_[b]->Finalize();
        WaitFor(stopLeader).ThrowOnError();
        WaitFor(stopB).ThrowOnError();

        int newLeaderId = WaitForLeader({a, c, d});
        TPeerServiceProxy proxy(Peers_[newLeaderId]->GetChannel());
        auto req = proxy.Read();
        req->SetTimeout(TDuration::Minutes(1));
        auto value = WaitFor(req->Invoke()).ValueOrThrow()->result();
        if (value != expectedValue) {
            YT_TLOG_ERROR("Acknowledged write lost")
                .With("AcknowledgedValue", expectedValue)
                .With("Value", value)
                .With("NewLeaderId", newLeaderId);
            return 1;
        }

        YT_TLOG_INFO("Acknowledged write survived")
            .With("Value", value)
            .With("NewLeaderId", newLeaderId);
        return 0;
    }

private:
    const TConfigPtr Config_;
    const TChannelManagerPtr ChannelManager_;
    const TActionQueuePtr SnapshotIOQueue_;
    std::vector<TPeerPtr> Peers_;

    TPeerChannelPtr Channel(int from, int to)
    {
        return ChannelManager_->GetChannel(from, to);
    }

    //! Returns false and logs the precondition that did not hold if #condition is not met in time.
    bool TryWaitForCondition(const std::function<bool()>& condition, TStringBuf what)
    {
        auto deadline = TInstant::Now() + TDuration::Minutes(3);
        while (!condition()) {
            if (TInstant::Now() > deadline) {
                YT_TLOG_ERROR("Timed out waiting for a condition; scenario is inconclusive")
                    .With("Condition", what);
                return false;
            }
            TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(50));
        }
        return true;
    }

    void WaitForCondition(const std::function<bool()>& condition, TStringBuf what)
    {
        if (!TryWaitForCondition(condition, what)) {
            THROW_ERROR_EXCEPTION("Timed out waiting for %v", what);
        }
    }

    //! Lets requests that passed the fault check before a fault change reach their destination;
    //! #TPeerChannel delays every request by up to 100 ms.
    void DrainChannels()
    {
        TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(200));
    }

    int WaitForLeader(const std::vector<int>& candidates)
    {
        int leaderId = -1;
        WaitForCondition(
            [&] {
                for (int id : candidates) {
                    if (Peers_[id]->GetAutomatonState() == NHydra::EPeerState::Leading && Peers_[id]->IsActive()) {
                        leaderId = id;
                        return true;
                    }
                }
                return false;
            },
            "an active leader");
        return leaderId;
    }

    TFuture<TPeerServiceProxy::TRspCasPtr> SendCas(int peerId, TValue expected, TValue desired)
    {
        TPeerServiceProxy proxy(Peers_[peerId]->GetChannel());
        auto req = proxy.Cas();
        req->SetTimeout(TDuration::Minutes(1));
        req->set_expected(expected);
        req->set_desired(desired);
        return req->Invoke();
    }
};

} // namespace

int RunFollowerRejoinScenario(const TString& workDir)
{
    if (NFS::Exists(workDir)) {
        NFS::RemoveRecursive(workDir);
    }
    NFS::MakeDirRecursive(workDir);

    NLogging::TLogManager::Get()->Configure(
        NLogging::TLogManagerConfig::CreateLogFile(workDir + "/scenario.log", NLogging::ELogLevel::Debug));

    auto queue = New<TActionQueue>("Scenario");
    auto resultOrError = BIND([&] {
            TScenario scenario(workDir);
            return scenario.Run();
        })
        .AsyncVia(queue->GetInvoker())
        .Run()
        .BlockingGet();

    int result = resultOrError.IsOK() ? resultOrError.Value() : 2;
    if (!resultOrError.IsOK()) {
        YT_TLOG_ERROR("Scenario failed with an error")
            .With(resultOrError);
    }
    NLogging::TLogManager::Get()->Synchronize();

    Cerr << (result == 0 ? "PASSED" : result == 1 ? "FAILED: acknowledged write lost" : "INCONCLUSIVE")
        << (resultOrError.IsOK() ? TString() : " (" + ToString(TError(resultOrError)) + ")")
        << Endl;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest
