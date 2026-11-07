#pragma once

#include "public.h"
#include "automaton.h"
#include "config.h"

#include <yt/yt/core/misc/public.h>

#include <yt/yt/server/lib/hydra/public.h>
#include <yt/yt/server/lib/hydra/hydra_service.h>

#include <yt/yt/client/hydra/public.h>
#include <yt/yt/client/hydra/version.h>

namespace NYT::NHydraStressTest {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TChangelogRecordCounts)

class TPeer
    : public TRefCounted
{
public:
    TPeer(
        TConfigPtr config,
        NElection::TCellConfigPtr cellConfig,
        NRpc::IChannelFactoryPtr channelFactory,
        int peerId,
        IInvokerPtr snapshotIOInvoker,
        TLinearizabilityCheckerPtr linearizabilityChecker,
        bool voting = true);

    void Initialize();
    TFuture<void> Finalize();

    bool IsActive() const;
    bool IsRecovery() const;
    bool IsVoting() const;

    //! Returns |(id, recordCount)| of the latest nonempty changelog of this peer, as written through
    //! its changelog store (zero version if there is none).
    NHydra::TVersion GetLastNonemptyLoggedVersion() const;

    //! Removes the peer's snapshots and changelogs. The peer must be finalized.
    void ClearPersistence();

    NHydra::EPeerState GetAutomatonState() const;
    NRpc::IChannelPtr GetChannel() const;
    int GetPeerId() const;
    IInvokerPtr GetAutomatonInvoker() const;

private:
    friend class TPeerService;

    const TConfigPtr Config_;
    const NElection::TCellConfigPtr CellConfig_;
    const IInvokerPtr SnapshotIOInvoker_;
    const TLinearizabilityCheckerPtr LinearizabilityChecker_;
    const int PeerId_;
    const bool Voting_;
    const NElection::TCellManagerPtr CellManager_;
    const NConcurrency::TActionQueuePtr AutomatonQueue_;
    const NConcurrency::TActionQueuePtr ControlQueue_;
    const NElection::TElectionManagerThunkPtr ElectionManagerThunk_;
    const NRpc::IServerPtr RpcServer_;
    const NRpc::IChannelPtr Channel_;
    const TChangelogRecordCountsPtr ChangelogRecordCounts_;

    NHydra::IChangelogStoreFactoryPtr ChangelogStoreFactory_;
    TAutomatonPartPtr AutomatonPart_;
    NHydra::IHydraManagerPtr HydraManager_;
    NRpc::IServicePtr PeerService_;
};

DEFINE_REFCOUNTED_TYPE(TPeer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest
