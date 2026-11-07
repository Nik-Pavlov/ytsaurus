#include "peer.h"
#include "peer_proxy.h"

#include <yt/yt/server/lib/hydra/changelog.h>
#include <yt/yt/server/lib/hydra/local_changelog_store.h>
#include <yt/yt/server/lib/hydra/local_snapshot_store.h>
#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>

#include <yt/yt/server/lib/election/distributed_election_manager.h>
#include <yt/yt/server/lib/election/election_manager_thunk.h>

#include <yt/yt/core/rpc/local_server.h>
#include <yt/yt/core/rpc/local_channel.h>

#include <yt/yt/core/utilex/random.h>

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/ytlib/election/cell_manager.h>

namespace NYT::NHydraStressTest {

using namespace NConcurrency;
using namespace NElection;
using namespace NHydra;
using namespace NRpc;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

//! Record counts of a peer's changelogs, as written through its changelog store.
struct TChangelogRecordCounts
    : public TRefCounted
{
    std::mutex Lock;
    std::map<int, int> ChangelogIdToRecordCount;

    void Update(const IChangelogPtr& changelog)
    {
        std::lock_guard guard(Lock);
        ChangelogIdToRecordCount[changelog->GetId()] = changelog->GetRecordCount();
    }

    void Set(int changelogId, int recordCount)
    {
        std::lock_guard guard(Lock);
        ChangelogIdToRecordCount[changelogId] = recordCount;
    }

    void Remove(int changelogId)
    {
        std::lock_guard guard(Lock);
        ChangelogIdToRecordCount.erase(changelogId);
    }
};

DEFINE_REFCOUNTED_TYPE(TChangelogRecordCounts)

namespace {

class TRecordCountTrackingChangelog
    : public IChangelog
{
public:
    TRecordCountTrackingChangelog(IChangelogPtr underlying, TChangelogRecordCountsPtr recordCounts)
        : Underlying_(std::move(underlying))
        , RecordCounts_(std::move(recordCounts))
    {
        RecordCounts_->Update(Underlying_);
    }

    int GetId() const override
    {
        return Underlying_->GetId();
    }

    const NHydra::NProto::TChangelogMeta& GetMeta() const override
    {
        return Underlying_->GetMeta();
    }

    int GetRecordCount() const override
    {
        return Underlying_->GetRecordCount();
    }

    i64 GetDataSize() const override
    {
        return Underlying_->GetDataSize();
    }

    i64 EstimateWriteSize(i64 payloadSize) const override
    {
        return Underlying_->EstimateWriteSize(payloadSize);
    }

    i64 EstimateReadSize(int firstRecordId, int maxRecords, i64 maxBytes) const override
    {
        return Underlying_->EstimateReadSize(firstRecordId, maxRecords, maxBytes);
    }

    TFuture<void> Append(TRange<TSharedRef> records) override
    {
        auto result = Underlying_->Append(records);
        // NB: Counted once flushed, since only then the peer may acknowledge the records.
        return result.Apply(BIND([id = GetId(), recordCount = Underlying_->GetRecordCount(), recordCounts = RecordCounts_] {
            recordCounts->Set(id, recordCount);
        }));
    }

    TFuture<void> Flush() override
    {
        return Underlying_->Flush();
    }

    TFuture<std::vector<TSharedRef>> Read(int firstRecordId, int maxRecords, i64 maxBytes) const override
    {
        return Underlying_->Read(firstRecordId, maxRecords, maxBytes);
    }

    TFuture<void> Truncate(int recordCount) override
    {
        return Underlying_->Truncate(recordCount)
            .Apply(BIND([underlying = Underlying_, recordCounts = RecordCounts_] {
                recordCounts->Update(underlying);
            }));
    }

    TFuture<void> Close() override
    {
        return Underlying_->Close();
    }

private:
    const IChangelogPtr Underlying_;
    const TChangelogRecordCountsPtr RecordCounts_;
};

class TRecordCountTrackingChangelogStore
    : public IChangelogStore
{
public:
    TRecordCountTrackingChangelogStore(IChangelogStorePtr underlying, TChangelogRecordCountsPtr recordCounts)
        : Underlying_(std::move(underlying))
        , RecordCounts_(std::move(recordCounts))
    { }

    bool IsReadOnly() const override
    {
        return Underlying_->IsReadOnly();
    }

    std::optional<int> TryGetTerm() const override
    {
        return Underlying_->TryGetTerm();
    }

    TFuture<void> SetTerm(int term) override
    {
        return Underlying_->SetTerm(term);
    }

    std::optional<int> TryGetLatestChangelogId() const override
    {
        return Underlying_->TryGetLatestChangelogId();
    }

    std::optional<TElectionPriority> GetElectionPriority() const override
    {
        return Underlying_->GetElectionPriority();
    }

    std::optional<TVersion> GetReachableVersion() const override
    {
        return Underlying_->GetReachableVersion();
    }

    TFuture<IChangelogPtr> CreateChangelog(
        int id,
        const NHydra::NProto::TChangelogMeta& meta,
        const TChangelogOptions& options) override
    {
        return Wrap(Underlying_->CreateChangelog(id, meta, options));
    }

    TFuture<IChangelogPtr> OpenChangelog(int id, const TChangelogOptions& options) override
    {
        return Wrap(Underlying_->OpenChangelog(id, options));
    }

    TFuture<void> RemoveChangelog(int id) override
    {
        return Underlying_->RemoveChangelog(id)
            .Apply(BIND([id, recordCounts = RecordCounts_] {
                recordCounts->Remove(id);
            }));
    }

    void Abort() override
    {
        Underlying_->Abort();
    }

private:
    const IChangelogStorePtr Underlying_;
    const TChangelogRecordCountsPtr RecordCounts_;

    TFuture<IChangelogPtr> Wrap(TFuture<IChangelogPtr> future)
    {
        return future.Apply(BIND([recordCounts = RecordCounts_] (const IChangelogPtr& changelog) -> IChangelogPtr {
            return New<TRecordCountTrackingChangelog>(changelog, recordCounts);
        }));
    }
};

class TRecordCountTrackingChangelogStoreFactory
    : public IChangelogStoreFactory
{
public:
    TRecordCountTrackingChangelogStoreFactory(IChangelogStoreFactoryPtr underlying, TChangelogRecordCountsPtr recordCounts)
        : Underlying_(std::move(underlying))
        , RecordCounts_(std::move(recordCounts))
    { }

    TFuture<IChangelogStorePtr> Lock() override
    {
        return Underlying_->Lock()
            .Apply(BIND([recordCounts = RecordCounts_] (const IChangelogStorePtr& store) -> IChangelogStorePtr {
                return New<TRecordCountTrackingChangelogStore>(store, recordCounts);
            }));
    }

    void Reconfigure(const TDynamicRemoteChangelogStoreConfigPtr& config) override
    {
        Underlying_->Reconfigure(config);
    }

private:
    const IChangelogStoreFactoryPtr Underlying_;
    const TChangelogRecordCountsPtr RecordCounts_;
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TPeerService
    : public THydraServiceBase
{
public:
    explicit TPeerService(TPeerPtr peer)
        : THydraServiceBase(
            peer->HydraManager_,
            peer->GetAutomatonInvoker(),
            TPeerServiceProxy::GetDescriptor(),
            HydraStressTestLogger(),
            CreateHydraManagerUpstreamSynchronizer(peer->HydraManager_))
        , Peer_(peer)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Read));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Cas));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Sequence));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ThrowException));
    }

private:
    const TPeerPtr Peer_;

    DECLARE_RPC_SERVICE_METHOD(NYT::NProto, Read)
    {
        Y_UNUSED(request);
        context->AnnotateRequest();
        ValidatePeer(EPeerKind::LeaderOrFollower);
        SyncWithUpstream();

        auto value = Peer_->AutomatonPart_->GetCasValue();
        response->set_result(value);
        context->AnnotateResponse()
            .With("Result", value);
        TDelayedExecutor::WaitForDuration(RandomDuration(TDuration::Seconds(3)));
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NYT::NProto, Cas)
    {
        Y_UNUSED(response);
        ValidatePeer(EPeerKind::Leader);

        context->AnnotateRequest()
            .With("Expected", request->expected())
            .With("Desired", request->desired());
        auto future = Peer_->AutomatonPart_->CreateCasMutation(context)->CommitAndReply(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NYT::NProto, Sequence)
    {
        Y_UNUSED(response);
        ValidatePeer(EPeerKind::Leader);

        context->AnnotateRequest()
            .With("Count", request->count())
            .With("SequenceId", request->id());
        auto future = Peer_->AutomatonPart_->CreateSequenceMutation(context)->CommitAndReply(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NYT::NProto, ThrowException)
    {
        Y_UNUSED(response);
        ValidatePeer(EPeerKind::Leader);

        auto future = Peer_->AutomatonPart_->CreateThrowExceptionMutation(context)->CommitAndReply(context);
    }
};

DEFINE_REFCOUNTED_TYPE(TPeerService)

////////////////////////////////////////////////////////////////////////////////

TPeer::TPeer(
    TConfigPtr config,
    TCellConfigPtr cellConfig,
    IChannelFactoryPtr channelFactory,
    int peerId,
    IInvokerPtr snapshotIOInvoker,
    TLinearizabilityCheckerPtr linearizabilityChecker,
    bool voting)
    : Config_(config)
    , CellConfig_(cellConfig)
    , SnapshotIOInvoker_(snapshotIOInvoker)
    , LinearizabilityChecker_(linearizabilityChecker)
    , PeerId_(peerId)
    , Voting_(voting)
    , CellManager_(New<TCellManager>(
        CellConfig_,
        channelFactory,
        nullptr,
        PeerId_))
    , AutomatonQueue_(New<TActionQueue>(Format("Automaton%v", PeerId_)))
    , ControlQueue_(New<TActionQueue>(Format("Control%v", PeerId_)))
    , ElectionManagerThunk_(New<TElectionManagerThunk>())
    , RpcServer_(CreateLocalServer())
    , Channel_(CreateLocalChannel(RpcServer_))
    , ChangelogRecordCounts_(New<TChangelogRecordCounts>())
{
    RpcServer_->Start();
    // NB: Not creating peer service here as that would require access to Hydra manager.
}

void TPeer::Initialize()
{
    auto automaton = New<TAutomaton>();

    // NB: Reused across restarts since the factory holds the changelog directory lock;
    // a new factory would fail to acquire it while the old one is alive.
    if (!ChangelogStoreFactory_) {
        auto changelogsConfig = CloneYsonStruct(Config_->Changelogs);
        changelogsConfig->Path += "/" + ToString(PeerId_);
        ChangelogStoreFactory_ = New<TRecordCountTrackingChangelogStoreFactory>(
            CreateLocalChangelogStoreFactory(
                changelogsConfig,
                Format("ChangelogFlush:%v", PeerId_),
                {}),
            ChangelogRecordCounts_);
    }

    auto snapshotsConfig = CloneYsonStruct(Config_->Snapshots);
    snapshotsConfig->Path += "/" + ToString(PeerId_);

    auto snapshotStore = WaitFor(CreateLocalSnapshotStore(snapshotsConfig, SnapshotIOInvoker_))
        .ValueOrThrow();

    HydraManager_ = NHydra::CreateDistributedHydraManager(
        Config_->HydraManager,
        ControlQueue_->GetInvoker(),
        AutomatonQueue_->GetInvoker(),
        automaton,
        RpcServer_,
        ElectionManagerThunk_,
        CellManager_->GetCellId(),
        CellManager_->GetTotalPeerCount(),
        ChangelogStoreFactory_,
        snapshotStore,
        /*authenticator*/ nullptr,
        {});

    AutomatonPart_ = New<TAutomatonPart>(
        HydraManager_,
        automaton,
        AutomatonQueue_->GetInvoker(),
        LinearizabilityChecker_);

    auto electionManager = CreateDistributedElectionManager(
        Config_->ElectionManager,
        CellManager_,
        ControlQueue_->GetInvoker(),
        HydraManager_->GetElectionCallbacks(),
        RpcServer_,
        /*authenticator*/ nullptr);
    ControlQueue_->GetInvoker()->Invoke(BIND(&IElectionManager::Initialize, electionManager));
    ElectionManagerThunk_->SetUnderlying(electionManager);

    PeerService_ = New<TPeerService>(this);
    RpcServer_->RegisterService(PeerService_);

    ControlQueue_->GetInvoker()->Invoke(BIND(&IHydraManager::Initialize, HydraManager_));
}

TFuture<void> TPeer::Finalize()
{
    if (PeerService_) {
        RpcServer_->UnregisterService(PeerService_);
        PeerService_.Reset();
    }

    auto hydraFinalized = BIND(&IHydraManager::Finalize, HydraManager_)
        .AsyncVia(ControlQueue_->GetInvoker())
        .Run();

    // NB: Wait for this too, otherwise it may finalize the election manager of the next Initialize call.
    auto electionFinalized = BIND(&TElectionManagerThunk::Finalize, ElectionManagerThunk_)
        .AsyncVia(ControlQueue_->GetInvoker())
        .Run();

    return AllSucceeded(std::vector{hydraFinalized, electionFinalized});
}

void TPeer::ClearPersistence()
{
    NFS::RemoveRecursive(Config_->Snapshots->Path + "/" + ToString(PeerId_));
    NFS::RemoveRecursive(Config_->Changelogs->Path + "/" + ToString(PeerId_));

    // The factory caches changelogs and holds the lock of the removed directory.
    ChangelogStoreFactory_.Reset();
    std::lock_guard guard(ChangelogRecordCounts_->Lock);
    ChangelogRecordCounts_->ChangelogIdToRecordCount.clear();
}

TVersion TPeer::GetLastNonemptyLoggedVersion() const
{
    std::lock_guard guard(ChangelogRecordCounts_->Lock);
    const auto& recordCounts = ChangelogRecordCounts_->ChangelogIdToRecordCount;
    for (auto it = recordCounts.rbegin(); it != recordCounts.rend(); ++it) {
        if (it->second > 0) {
            return TVersion(it->first, it->second);
        }
    }
    return TVersion();
}

bool TPeer::IsActive() const
{
    return HydraManager_->IsActive();
}

bool TPeer::IsRecovery() const
{
    auto future = BIND(&IHydraManager::IsRecovery, HydraManager_)
        .AsyncVia(AutomatonQueue_->GetInvoker())
        .Run();
    return WaitFor(future).ValueOrThrow();
}

bool TPeer::IsVoting() const
{
    return Voting_;
}

NHydra::EPeerState TPeer::GetAutomatonState() const
{
    auto future = BIND(&IHydraManager::GetAutomatonState, HydraManager_)
        .AsyncVia(AutomatonQueue_->GetInvoker())
        .Run();
    return WaitFor(future).ValueOrThrow();
}

IChannelPtr TPeer::GetChannel() const
{
    return Channel_;
}

int TPeer::GetPeerId() const
{
    return PeerId_;
}

IInvokerPtr TPeer::GetAutomatonInvoker() const
{
    return AutomatonQueue_->GetInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest
