using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoDB.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for BulkWrite operations.
/// </summary>
[Collection("Integration")]
public class BulkWriteTests : IAsyncLifetime
{
    private readonly MongoDbSession _session;
    private ITestCollectionFixture _fixture = null!;

    public BulkWriteTests(MongoDbSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        await _fixture.ResetAsync();
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task BulkWrite_MixedOperations()
    {
        var col = _fixture.GetCollection<BsonDocument>("bulkwrite_1");
        await col.InsertOneAsync(new BsonDocument { { "name", "Alice" }, { "value", 1 } });
        await col.InsertOneAsync(new BsonDocument { { "name", "Bob" }, { "value", 2 } });

        var result = await col.BulkWriteAsync(new WriteModel<BsonDocument>[]
        {
            new InsertOneModel<BsonDocument>(new BsonDocument { { "name", "Charlie" }, { "value", 3 } }),
            new UpdateOneModel<BsonDocument>(
                Builders<BsonDocument>.Filter.Eq("name", "Alice"),
                Builders<BsonDocument>.Update.Set("value", 99)),
            new DeleteOneModel<BsonDocument>(Builders<BsonDocument>.Filter.Eq("name", "Bob"))
        });

        Assert.True(result.IsAcknowledged);
        Assert.Equal(1, result.InsertedCount);
        Assert.Equal(1, result.MatchedCount);
        Assert.Equal(1, result.ModifiedCount);
        Assert.Equal(1, result.DeletedCount);

        var count = await col.CountDocumentsAsync(Builders<BsonDocument>.Filter.Empty);
        Assert.Equal(2, count);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task BulkWrite_InsertOnly()
    {
        var col = _fixture.GetCollection<BsonDocument>("bulkwrite_1");

        var result = await col.BulkWriteAsync(new WriteModel<BsonDocument>[]
        {
            new InsertOneModel<BsonDocument>(new BsonDocument { { "n", 1 } }),
            new InsertOneModel<BsonDocument>(new BsonDocument { { "n", 2 } }),
            new InsertOneModel<BsonDocument>(new BsonDocument { { "n", 3 } })
        });

        Assert.Equal(3, result.InsertedCount);
        Assert.Equal(3, await col.CountDocumentsAsync(Builders<BsonDocument>.Filter.Empty));
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task BulkWrite_ReplaceOne()
    {
        var col = _fixture.GetCollection<BsonDocument>("bulkwrite_1");
        await col.InsertOneAsync(new BsonDocument { { "name", "Alice" }, { "value", 1 } });

        var result = await col.BulkWriteAsync(new WriteModel<BsonDocument>[]
        {
            new ReplaceOneModel<BsonDocument>(
                Builders<BsonDocument>.Filter.Eq("name", "Alice"),
                new BsonDocument { { "name", "Alice" }, { "value", 999 } })
        });

        Assert.Equal(1, result.MatchedCount);
        Assert.Equal(1, result.ModifiedCount);

        var doc = await col.Find(Builders<BsonDocument>.Filter.Eq("name", "Alice")).FirstOrDefaultAsync();
        Assert.Equal(999, doc["value"].AsInt32);
    }
}
