using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;
using AwesomeAssertions;

namespace MongoDB.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Phase 4 integration tests: Change streams (Watch) on collection, database, and client.
/// Uses BsonDocument output pipeline for raw access to change events.
/// </summary>
[Collection("Integration")]
public class ChangeStreamTests : IAsyncLifetime
{
    private readonly MongoDbSession _session;
    private ITestCollectionFixture _fixture = null!;

    public ChangeStreamTests(MongoDbSession session)
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

    // Helper: create an identity pipeline that outputs BsonDocument for easier assertion.
    private static PipelineDefinition<ChangeStreamDocument<BsonDocument>, BsonDocument> RawPipeline()
    {
        return new EmptyPipelineDefinition<ChangeStreamDocument<BsonDocument>>()
            .As<ChangeStreamDocument<BsonDocument>, ChangeStreamDocument<BsonDocument>, BsonDocument>();
    }

    #region Collection-level Watch

    [Fact]
    public async Task Watch_collection_receives_insert_event()
    {
        // Ref: https://www.mongodb.com/docs/manual/changeStreams/
        //   "You can open change streams against collections, databases, and deployments."
        var collection = _fixture.GetCollection<BsonDocument>("cs_insert");

        using var cursor = collection.Watch(RawPipeline());

        await collection.InsertOneAsync(new BsonDocument { { "name", "Alice" } });

        var hasEvents = cursor.MoveNext();
        Assert.True(hasEvents);
        var events = cursor.Current.ToList();
        Assert.Single(events);

        var evt = events[0];
        Assert.Equal("insert", evt["operationType"].AsString);
        Assert.Equal("Alice", evt["fullDocument"]["name"].AsString);
    }

    [Fact]
    public async Task Watch_collection_receives_update_event()
    {
        var collection = _fixture.GetCollection<BsonDocument>("cs_update");
        await collection.InsertOneAsync(new BsonDocument { { "_id", "u1" }, { "name", "Bob" } });

        using var cursor = collection.Watch(RawPipeline());

        await collection.UpdateOneAsync(
            Builders<BsonDocument>.Filter.Eq("_id", "u1"),
            Builders<BsonDocument>.Update.Set("name", "Robert"));

        var hasEvents = cursor.MoveNext();
        Assert.True(hasEvents);
        var evt = cursor.Current.First();
        Assert.Equal("update", evt["operationType"].AsString);
    }

    [Fact]
    public async Task Watch_collection_receives_delete_event()
    {
        var collection = _fixture.GetCollection<BsonDocument>("cs_delete");
        await collection.InsertOneAsync(new BsonDocument { { "_id", "d1" }, { "name", "Charlie" } });

        using var cursor = collection.Watch(RawPipeline());

        await collection.DeleteOneAsync(Builders<BsonDocument>.Filter.Eq("_id", "d1"));

        var hasEvents = cursor.MoveNext();
        Assert.True(hasEvents);
        var evt = cursor.Current.First();
        Assert.Equal("delete", evt["operationType"].AsString);
        Assert.Equal("d1", evt["documentKey"]["_id"].AsString);
    }

    [Fact]
    public async Task Watch_collection_receives_replace_event()
    {
        var collection = _fixture.GetCollection<BsonDocument>("cs_replace");
        await collection.InsertOneAsync(new BsonDocument { { "_id", "r1" }, { "name", "Old" } });

        using var cursor = collection.Watch(RawPipeline());

        await collection.ReplaceOneAsync(
            Builders<BsonDocument>.Filter.Eq("_id", "r1"),
            new BsonDocument { { "_id", "r1" }, { "name", "New" } });

        var hasEvents = cursor.MoveNext();
        Assert.True(hasEvents);
        var evt = cursor.Current.First();
        Assert.Equal("replace", evt["operationType"].AsString);
        Assert.Equal("New", evt["fullDocument"]["name"].AsString);
    }

    [Fact]
    public async Task Watch_collection_multiple_events_in_order()
    {
        var collection = _fixture.GetCollection<BsonDocument>("cs_multi");

        using var cursor = collection.Watch(RawPipeline());

        await collection.InsertOneAsync(new BsonDocument { { "_id", "m1" }, { "val", 1 } });
        await collection.InsertOneAsync(new BsonDocument { { "_id", "m2" }, { "val", 2 } });
        await collection.UpdateOneAsync(
            Builders<BsonDocument>.Filter.Eq("_id", "m1"),
            Builders<BsonDocument>.Update.Set("val", 10));

        var hasEvents = cursor.MoveNext();
        Assert.True(hasEvents);
        var events = cursor.Current.ToList();
        Assert.Equal(3, events.Count);
        Assert.Equal("insert", events[0]["operationType"].AsString);
        Assert.Equal("insert", events[1]["operationType"].AsString);
        Assert.Equal("update", events[2]["operationType"].AsString);
    }

    #endregion

    #region Resume Token

    [Fact]
    public async Task Watch_resume_token_allows_resuming()
    {
        // Ref: https://www.mongodb.com/docs/manual/changeStreams/#resume-a-change-stream
        //   "Each change stream event document includes a resume token."
        var collection = _fixture.GetCollection<BsonDocument>("cs_resume");

        using var cursor1 = collection.Watch(RawPipeline());

        await collection.InsertOneAsync(new BsonDocument { { "_id", "rt1" }, { "val", 1 } });
        cursor1.MoveNext();
        var token = cursor1.GetResumeToken();

        // Insert more after getting token
        await collection.InsertOneAsync(new BsonDocument { { "_id", "rt2" }, { "val", 2 } });

        // Resume from token
        var options = new ChangeStreamOptions { ResumeAfter = token };
        using var cursor2 = collection.Watch(RawPipeline(), options);

        var hasEvents = cursor2.MoveNext();
        Assert.True(hasEvents);
        var events = cursor2.Current.ToList();
        Assert.Single(events);
        Assert.Equal("rt2", events[0]["fullDocument"]["_id"].AsString);
    }

    #endregion

    #region Database-level Watch

    [Fact]
    public async Task Watch_database_receives_events_from_all_collections()
    {
        // Ref: https://www.mongodb.com/docs/manual/changeStreams/
        //   "You can open a change stream cursor for a single database."
        var db = _fixture.Database;

        var dbPipeline = new EmptyPipelineDefinition<ChangeStreamDocument<BsonDocument>>()
            .As<ChangeStreamDocument<BsonDocument>, ChangeStreamDocument<BsonDocument>, BsonDocument>();
        using var cursor = db.Watch(dbPipeline);

        var coll1 = db.GetCollection<BsonDocument>("cs_db1");
        var coll2 = db.GetCollection<BsonDocument>("cs_db2");

        await coll1.InsertOneAsync(new BsonDocument { { "from", "coll1" } });
        await coll2.InsertOneAsync(new BsonDocument { { "from", "coll2" } });

        var hasEvents = cursor.MoveNext();
        Assert.True(hasEvents);
        var events = cursor.Current.ToList();
        Assert.Equal(2, events.Count);
    }

    #endregion

    #region Client-level Watch

    [Fact]
    public async Task Watch_client_receives_events_from_all_databases()
    {
        // Ref: https://www.mongodb.com/docs/manual/changeStreams/
        //   "Starting in MongoDB 4.0, you can open a change stream cursor for a deployment."
        var client = _fixture.Client;

        var clientPipeline = new EmptyPipelineDefinition<ChangeStreamDocument<BsonDocument>>()
            .As<ChangeStreamDocument<BsonDocument>, ChangeStreamDocument<BsonDocument>, BsonDocument>();
        using var cursor = client.Watch(clientPipeline);

        var db1 = client.GetDatabase("cs_client_db1");
        var db2 = client.GetDatabase("cs_client_db2");

        await db1.GetCollection<BsonDocument>("test").InsertOneAsync(new BsonDocument { { "db", 1 } });
        await db2.GetCollection<BsonDocument>("test").InsertOneAsync(new BsonDocument { { "db", 2 } });

        var hasEvents = cursor.MoveNext();
        Assert.True(hasEvents);
        var events = cursor.Current.ToList();
        Assert.True(events.Count >= 2);
    }

    #endregion

    #region Async

    [Fact]
    public async Task WatchAsync_collection_receives_insert()
    {
        var collection = _fixture.GetCollection<BsonDocument>("cs_async");

        using var cursor = await collection.WatchAsync(RawPipeline());

        await collection.InsertOneAsync(new BsonDocument { { "name", "AsyncTest" } });

        var hasEvents = await cursor.MoveNextAsync();
        Assert.True(hasEvents);
        Assert.Single(cursor.Current);
    }

    #endregion

    #region GetResumeToken

    [Fact]
    public async Task GetResumeToken_returns_sequence_based_token()
    {
        var collection = _fixture.GetCollection<BsonDocument>("cs_token");

        using var cursor = collection.Watch(RawPipeline());

        await collection.InsertOneAsync(new BsonDocument { { "x", 1 } });
        cursor.MoveNext();

        var token = cursor.GetResumeToken();
        Assert.NotNull(token);
        Assert.True(token.Contains("_seq"));
        Assert.True(token["_seq"].ToInt64() > 0);
    }

    #endregion
}
