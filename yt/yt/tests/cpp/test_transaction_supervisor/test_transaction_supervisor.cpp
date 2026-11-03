#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/transaction_supervisor/transaction_supervisor_service_proxy.h>

#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/rpc/helpers.h>

namespace NYT::NCppTests {
namespace {

using namespace NApi;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NRpc;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

// NB: The cluster is configured (see config.yson) with
// tablet_node/transaction_supervisor/testing_finish_delay = 5s, which only widens
// the window between the coordinator's phase two and its Finish mutation.

class TTransactionSupervisorTest
    : public TApiTestBase
{
public:
    static void SetUpTestCase()
    {
        TApiTestBase::SetUpTestCase();

        WaitFor(Client_->SetNode("//sys/accounts/tmp/@resource_limits/tablet_count", ConvertToYsonString(1000)))
            .ThrowOnError();

        for (int index = 0; index < 2; ++index) {
            auto cellId = WaitFor(Client_->CreateObject(EObjectType::TabletCell))
                .ValueOrThrow();
            WaitUntilEqual(Format("#%v/@health", cellId), "good");
            CellIds_.push_back(cellId);
        }
    }

protected:
    static inline std::vector<TTabletCellId> CellIds_;

    static void CreateMountedTable(const TYPath& path, TTabletCellId cellId)
    {
        TCreateNodeOptions options;
        options.Force = true;
        options.Attributes = ConvertToAttributes(TYsonString(TStringBuf(
            "{dynamic=%true;schema=[{name=key;type=int64;sort_order=ascending};{name=value;type=string}]}")));
        WaitFor(Client_->CreateNode(path, EObjectType::Table, options))
            .ThrowOnError();

        TMountTableOptions mountOptions;
        mountOptions.CellId = cellId;
        WaitFor(Client_->MountTable(path, mountOptions))
            .ThrowOnError();
        WaitUntilEqual(path + "/@tablet_state", "mounted");
    }

    static void WriteRow(const ITransactionPtr& transaction, const TYPath& path, i64 key, const std::string& value)
    {
        auto nameTable = New<TNameTable>();
        nameTable->RegisterName("key");
        nameTable->RegisterName("value");
        TUnversionedRowsBuilder builder;
        builder.AddRow(key, value);
        transaction->WriteRows(path, nameTable, builder.Build());
    }

    static bool HasRow(const TYPath& path, i64 key)
    {
        auto nameTable = New<TNameTable>();
        nameTable->RegisterName("key");
        TUnversionedRowsBuilder builder;
        builder.AddRow(key);
        auto result = WaitFor(Client_->LookupRows(path, nameTable, builder.Build()))
            .ValueOrThrow();
        return !result.Rowset->GetRows().empty();
    }

    static IChannelPtr GetCellChannel(TCellId cellId)
    {
        auto connection = DynamicPointerCast<NNative::IConnection>(Connection_);
        YT_VERIFY(connection);
        return connection->GetCellDirectory()->GetChannelByCellIdOrThrow(cellId);
    }

    static TError ForceAbortAtCell(TTransactionId transactionId, TCellId cellId)
    {
        NTransactionSupervisor::TTransactionSupervisorServiceProxy proxy(GetCellChannel(cellId));
        auto req = proxy.AbortTransaction();
        req->SetUser(RootUserName);
        ToProto(req->mutable_transaction_id(), transactionId);
        req->set_force(true);
        return WaitFor(req->Invoke());
    }

    static TError RetryCommitAtCell(TTransactionId transactionId, TCellId cellId, TMutationId mutationId)
    {
        NTransactionSupervisor::TTransactionSupervisorServiceProxy proxy(GetCellChannel(cellId));
        auto req = proxy.CommitTransaction();
        req->SetUser(RootUserName);
        ToProto(req->mutable_transaction_id(), transactionId);
        SetMutationId(req, mutationId, /*retry*/ true);
        return WaitFor(req->Invoke());
    }
};

////////////////////////////////////////////////////////////////////////////////

// With Early coordinator prepare and Lazy coordinator commit, the coordinator stays
// persistently prepared during phase two, until its Finish mutation. A forced abort
// in that window must not abort the coordinator: participants may already have committed.
TEST_F(TTransactionSupervisorTest, ForceAbortAtCoordinatorDuringLazyPhaseTwo)
{
    auto coordinatorPath = TYPath("//tmp/t_coordinator");
    auto participantPath = TYPath("//tmp/t_participant");
    CreateMountedTable(coordinatorPath, CellIds_[0]);
    CreateMountedTable(participantPath, CellIds_[1]);

    auto transaction = WaitFor(Client_->StartTransaction(ETransactionType::Tablet))
        .ValueOrThrow();
    WriteRow(transaction, coordinatorPath, 1, "coordinator");
    WriteRow(transaction, participantPath, 1, "participant");

    auto mutationId = TMutationId::Create();

    TTransactionCommitOptions commitOptions;
    commitOptions.CoordinatorCellId = CellIds_[0];
    commitOptions.CoordinatorPrepareMode = ETransactionCoordinatorPrepareMode::Early;
    commitOptions.CoordinatorCommitMode = ETransactionCoordinatorCommitMode::Lazy;
    commitOptions.MutationId = mutationId;
    auto commitFuture = transaction->Commit(commitOptions);

    // Phase two has started and the participant has committed; the coordinator's Finish is delayed.
    WaitUntil(
        [&] { return HasRow(participantPath, 1); },
        "Participant did not commit");
    ASSERT_FALSE(commitFuture.IsSet());

    auto abortError = ForceAbortAtCell(transaction->GetId(), CellIds_[0]);
    Cerr << "Forced abort at coordinator: " << ToString(abortError) << Endl;

    auto commitResult = WaitFor(commitFuture);
    Cerr << "Commit result: " << ToString(TError(commitResult)) << Endl;

    bool hasCoordinatorRow = HasRow(coordinatorPath, 1);
    bool hasParticipantRow = HasRow(participantPath, 1);
    Cerr << "Rows: coordinator=" << hasCoordinatorRow << " participant=" << hasParticipantRow << Endl;

    // Atomicity: the participant has committed, so the coordinator must commit too.
    EXPECT_EQ(hasCoordinatorRow, hasParticipantRow);
    // The outcome reported to the client must match the data.
    EXPECT_EQ(commitResult.IsOK(), hasParticipantRow) << ToString(TError(commitResult));

    // Once the coordinator has finished, a retry with the original mutation id
    // must report the same outcome.
    auto commitPath = Format("//sys/tablet_cells/%v/orchid/transaction_supervisor/persistent_commits/%v",
        CellIds_[0],
        transaction->GetId());
    WaitUntil(
        [&] { return !WaitFor(Client_->NodeExists(commitPath)).ValueOrThrow(); },
        "Coordinator did not finish the commit");
    auto retryError = RetryCommitAtCell(transaction->GetId(), CellIds_[0], mutationId);
    Cerr << "Commit retry with the original mutation id: " << ToString(retryError) << Endl;
    EXPECT_EQ(retryError.IsOK(), HasRow(coordinatorPath, 1));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCppTests
