using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoDB.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for Distinct operations.
/// </summary>
[Collection("Integration")]
public class DistinctTests : IAsyncLifetime
{
    private readonly MongoDbSession _session;
    private ITestCollectionFixture _fixture = null!;

    public DistinctTests(MongoDbSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        await _fixture.ResetAsync();
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Distinct_ReturnsUniqueValues()
    {
        var col = _fixture.GetCollection<BsonDocument>("distinct_1");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "category", "A" } },
            new BsonDocument { { "category", "B" } },
            new BsonDocument { { "category", "A" } },
            new BsonDocument { { "category", "C" } },
            new BsonDocument { { "category", "B" } }
        });

        var cursor = await col.DistinctAsync<string>("category", Builders<BsonDocument>.Filter.Empty);
        var values = await cursor.ToListAsync();

        Assert.Equal(3, values.Count);
        Assert.Contains("A", values);
        Assert.Contains("B", values);
        Assert.Contains("C", values);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Distinct_WithFilter()
    {
        var col = _fixture.GetCollection<BsonDocument>("distinct_1");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "category", "A" }, { "active", true } },
            new BsonDocument { { "category", "B" }, { "active", false } },
            new BsonDocument { { "category", "A" }, { "active", true } },
            new BsonDocument { { "category", "C" }, { "active", true } }
        });

        var cursor = await col.DistinctAsync<string>("category",
            Builders<BsonDocument>.Filter.Eq("active", true));
        var values = await cursor.ToListAsync();

        Assert.Equal(2, values.Count);
        Assert.Contains("A", values);
        Assert.Contains("C", values);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Distinct_IntegerField()
    {
        var col = _fixture.GetCollection<BsonDocument>("distinct_1");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "score", 1 } },
            new BsonDocument { { "score", 2 } },
            new BsonDocument { { "score", 1 } },
            new BsonDocument { { "score", 3 } }
        });

        var cursor = await col.DistinctAsync<int>("score", Builders<BsonDocument>.Filter.Empty);
        var values = await cursor.ToListAsync();

        Assert.Equal(3, values.Count);
        Assert.Contains(1, values);
        Assert.Contains(2, values);
        Assert.Contains(3, values);
    }
}
