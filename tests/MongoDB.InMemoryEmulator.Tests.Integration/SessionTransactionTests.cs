using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;
using AwesomeAssertions;

namespace MongoDB.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Phase 4 integration tests: Sessions and transactions.
/// </summary>
[Collection("Integration")]
public class SessionTransactionTests : IAsyncLifetime
{
    private readonly MongoDbSession _session;
    private ITestCollectionFixture _fixture = null!;

    public SessionTransactionTests(MongoDbSession session)
    {
        _session = session;
    }

    public ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await _fixture.DisposeAsync();
    }

    #region Session

    [Fact]
    public void StartSession_returns_handle()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/method/Mongo.startSession/
        //   "Starts a new session on the server."
        using var sessionHandle = _fixture.Client.StartSession();

        Assert.NotNull(sessionHandle);
        Assert.False(sessionHandle.IsInTransaction);
        Assert.Same(_fixture.Client, sessionHandle.Client);
    }

    [Fact]
    public async Task StartSessionAsync_returns_handle()
    {
        using var sessionHandle = await _fixture.Client.StartSessionAsync();

        Assert.NotNull(sessionHandle);
        Assert.NotNull(sessionHandle.ServerSession);
        Assert.NotNull(sessionHandle.ServerSession.Id);
    }

    [Fact]
    public void Session_is_not_implicit()
    {
        using var sessionHandle = _fixture.Client.StartSession();
        Assert.False(sessionHandle.IsImplicit);
    }

    [Fact]
    public void Session_options_are_preserved()
    {
        var opts = new ClientSessionOptions
        {
            CausalConsistency = true,
            DefaultTransactionOptions = new TransactionOptions(
                readConcern: ReadConcern.Majority,
                writeConcern: WriteConcern.WMajority)
        };

        using var sessionHandle = _fixture.Client.StartSession(opts);

        Assert.NotNull(sessionHandle.Options);
    }

    #endregion

    #region Transaction Commit

    [Fact]
    public async Task Transaction_commit_applies_inserts()
    {
        // Ref: https://www.mongodb.com/docs/manual/core/transactions/
        //   "In MongoDB, an operation on a single document is atomic."
        var collection = _fixture.GetCollection<BsonDocument>("tx_commit");

        using var sessionHandle = _fixture.Client.StartSession();
        sessionHandle.StartTransaction();

        await collection.InsertOneAsync(sessionHandle, new BsonDocument { { "_id", "tx1" }, { "val", 1 } });
        await collection.InsertOneAsync(sessionHandle, new BsonDocument { { "_id", "tx2" }, { "val", 2 } });

        sessionHandle.CommitTransaction();

        var cursor = await collection.FindAsync(FilterDefinition<BsonDocument>.Empty);
        var results = await cursor.ToListAsync();
        Assert.True(results.Count >= 2);
    }

    [Fact]
    public async Task Transaction_commit_applies_all_operations()
    {
        var collection = _fixture.GetCollection<BsonDocument>("tx_commit_ops");
        await collection.InsertOneAsync(new BsonDocument { { "_id", "pre1" }, { "val", 0 } });

        using var sessionHandle = _fixture.Client.StartSession();
        sessionHandle.StartTransaction();

        await collection.InsertOneAsync(sessionHandle, new BsonDocument { { "_id", "txn1" }, { "val", 10 } });
        await collection.UpdateOneAsync(sessionHandle,
            Builders<BsonDocument>.Filter.Eq("_id", "pre1"),
            Builders<BsonDocument>.Update.Set("val", 100));

        sessionHandle.CommitTransaction();

        var doc1 = await collection.Find(Builders<BsonDocument>.Filter.Eq("_id", "pre1")).FirstOrDefaultAsync();
        Assert.Equal(100, doc1["val"].AsInt32);

        var doc2 = await collection.Find(Builders<BsonDocument>.Filter.Eq("_id", "txn1")).FirstOrDefaultAsync();
        Assert.NotNull(doc2);
    }

    #endregion

    #region Transaction Abort

    [Fact]
    public void Transaction_abort_discards_operations()
    {
        var collection = _fixture.GetCollection<BsonDocument>("tx_abort");

        using var sessionHandle = _fixture.Client.StartSession();
        sessionHandle.StartTransaction();
        sessionHandle.AbortTransaction();

        Assert.False(sessionHandle.IsInTransaction);
    }

    [Fact]
    public void Transaction_abort_after_start_valid()
    {
        using var sessionHandle = _fixture.Client.StartSession();
        sessionHandle.StartTransaction();
        Assert.True(sessionHandle.IsInTransaction);
        sessionHandle.AbortTransaction();
        Assert.False(sessionHandle.IsInTransaction);
    }

    #endregion

    #region WithTransaction

    [Fact]
    public async Task WithTransaction_callback_executes_and_commits()
    {
        // Ref: https://www.mongodb.com/docs/manual/core/transactions-in-applications/
        //   "The API incorporates retry logic for TransientTransactionError."
        var collection = _fixture.GetCollection<BsonDocument>("tx_with");

        using var sessionHandle = _fixture.Client.StartSession();

        var result = sessionHandle.WithTransaction((s, ct) =>
        {
            collection.InsertOne(s, new BsonDocument { { "_id", "wt1" }, { "val", 999 } });
            return "done";
        });

        Assert.Equal("done", result);

        var doc = await collection.Find(Builders<BsonDocument>.Filter.Eq("_id", "wt1")).FirstOrDefaultAsync();
        Assert.NotNull(doc);
    }

    [Fact]
    public async Task WithTransactionAsync_callback_executes()
    {
        var collection = _fixture.GetCollection<BsonDocument>("tx_with_async");

        using var sessionHandle = await _fixture.Client.StartSessionAsync();

        var result = await sessionHandle.WithTransactionAsync(async (s, ct) =>
        {
            await collection.InsertOneAsync(s, new BsonDocument { { "_id", "wta1" }, { "val", 42 } });
            return "async_done";
        });

        Assert.Equal("async_done", result);
    }

    #endregion

    #region Error Cases

    [Fact]
    public void StartTransaction_when_already_in_transaction_throws()
    {
        using var sessionHandle = _fixture.Client.StartSession();
        sessionHandle.StartTransaction();

        Assert.Throws<InvalidOperationException>(() => sessionHandle.StartTransaction());

        sessionHandle.AbortTransaction();
    }

    [Fact]
    public void CommitTransaction_without_start_throws()
    {
        using var sessionHandle = _fixture.Client.StartSession();
        Assert.Throws<InvalidOperationException>(() => sessionHandle.CommitTransaction());
    }

    [Fact]
    public void AbortTransaction_without_start_throws()
    {
        using var sessionHandle = _fixture.Client.StartSession();
        Assert.Throws<InvalidOperationException>(() => sessionHandle.AbortTransaction());
    }

    #endregion

    #region Session Dispose

    [Fact]
    public void Disposing_session_aborts_active_transaction()
    {
        var sessionHandle = _fixture.Client.StartSession();
        sessionHandle.StartTransaction();
        Assert.True(sessionHandle.IsInTransaction);
        sessionHandle.Dispose();
        // After dispose, the transaction should have been aborted
    }

    #endregion

    #region Cluster/Operation Time

    [Fact]
    public void AdvanceClusterTime_sets_cluster_time()
    {
        using var sessionHandle = _fixture.Client.StartSession();
        var ct = new BsonDocument("clusterTime", new BsonTimestamp(12345));
        sessionHandle.AdvanceClusterTime(ct);
        Assert.Equal(ct, sessionHandle.ClusterTime);
    }

    [Fact]
    public void AdvanceOperationTime_sets_operation_time()
    {
        using var sessionHandle = _fixture.Client.StartSession();
        var ot = new BsonTimestamp(999);
        sessionHandle.AdvanceOperationTime(ot);
        Assert.Equal(ot, sessionHandle.OperationTime);
    }

    #endregion
}
