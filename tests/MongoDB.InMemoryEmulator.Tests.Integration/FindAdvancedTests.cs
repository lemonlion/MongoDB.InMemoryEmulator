using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoDB.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for Find with sort, skip, limit, and projection.
/// </summary>
[Collection("Integration")]
public class FindAdvancedTests : IAsyncLifetime
{
    private readonly MongoDbSession _session;
    private ITestCollectionFixture _fixture = null!;

    public FindAdvancedTests(MongoDbSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        await _fixture.ResetAsync();
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    #region Sort

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Find_SortAscending_ReturnsSorted()
    {
        var col = _fixture.GetCollection<TestDoc>("findadvanced_1");
        await col.InsertManyAsync(new[]
        {
            new TestDoc { Name = "C", Value = 3 },
            new TestDoc { Name = "A", Value = 1 },
            new TestDoc { Name = "B", Value = 2 }
        });

        var docs = await col.Find(Builders<TestDoc>.Filter.Empty)
            .Sort(Builders<TestDoc>.Sort.Ascending(x => x.Name))
            .ToListAsync();

        Assert.Equal("A", docs[0].Name);
        Assert.Equal("B", docs[1].Name);
        Assert.Equal("C", docs[2].Name);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Find_SortDescending_ReturnsSorted()
    {
        var col = _fixture.GetCollection<TestDoc>("findadvanced_1");
        await col.InsertManyAsync(new[]
        {
            new TestDoc { Name = "A", Value = 1 },
            new TestDoc { Name = "C", Value = 3 },
            new TestDoc { Name = "B", Value = 2 }
        });

        var docs = await col.Find(Builders<TestDoc>.Filter.Empty)
            .Sort(Builders<TestDoc>.Sort.Descending(x => x.Value))
            .ToListAsync();

        Assert.Equal(3, docs[0].Value);
        Assert.Equal(2, docs[1].Value);
        Assert.Equal(1, docs[2].Value);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Find_MultiFieldSort()
    {
        var col = _fixture.GetCollection<TestDoc>("findadvanced_1");
        await col.InsertManyAsync(new[]
        {
            new TestDoc { Name = "A", Value = 2 },
            new TestDoc { Name = "A", Value = 1 },
            new TestDoc { Name = "B", Value = 1 }
        });

        var docs = await col.Find(Builders<TestDoc>.Filter.Empty)
            .Sort(Builders<TestDoc>.Sort.Ascending(x => x.Name).Descending(x => x.Value))
            .ToListAsync();

        Assert.Equal("A", docs[0].Name);
        Assert.Equal(2, docs[0].Value);
        Assert.Equal("A", docs[1].Name);
        Assert.Equal(1, docs[1].Value);
        Assert.Equal("B", docs[2].Name);
    }

    #endregion

    #region Skip / Limit

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Find_Skip_SkipsDocuments()
    {
        var col = _fixture.GetCollection<TestDoc>("findadvanced_1");
        await col.InsertManyAsync(new[]
        {
            new TestDoc { Name = "A", Value = 1 },
            new TestDoc { Name = "B", Value = 2 },
            new TestDoc { Name = "C", Value = 3 }
        });

        var docs = await col.Find(Builders<TestDoc>.Filter.Empty)
            .Sort(Builders<TestDoc>.Sort.Ascending(x => x.Value))
            .Skip(1)
            .ToListAsync();

        Assert.Equal(2, docs.Count);
        Assert.Equal(2, docs[0].Value);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Find_Limit_LimitsDocuments()
    {
        var col = _fixture.GetCollection<TestDoc>("findadvanced_1");
        await col.InsertManyAsync(new[]
        {
            new TestDoc { Name = "A", Value = 1 },
            new TestDoc { Name = "B", Value = 2 },
            new TestDoc { Name = "C", Value = 3 }
        });

        var docs = await col.Find(Builders<TestDoc>.Filter.Empty)
            .Sort(Builders<TestDoc>.Sort.Ascending(x => x.Value))
            .Limit(2)
            .ToListAsync();

        Assert.Equal(2, docs.Count);
        Assert.Equal(1, docs[0].Value);
        Assert.Equal(2, docs[1].Value);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Find_SkipAndLimit_Combined()
    {
        var col = _fixture.GetCollection<TestDoc>("findadvanced_1");
        for (int i = 1; i <= 10; i++)
            await col.InsertOneAsync(new TestDoc { Name = $"Doc{i}", Value = i });

        var docs = await col.Find(Builders<TestDoc>.Filter.Empty)
            .Sort(Builders<TestDoc>.Sort.Ascending(x => x.Value))
            .Skip(3)
            .Limit(3)
            .ToListAsync();

        Assert.Equal(3, docs.Count);
        Assert.Equal(4, docs[0].Value);
        Assert.Equal(5, docs[1].Value);
        Assert.Equal(6, docs[2].Value);
    }

    #endregion

    #region Projection

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Find_Projection_IncludeFields()
    {
        var col = _fixture.GetCollection<BsonDocument>("findadvanced_1");
        await col.InsertOneAsync(new BsonDocument { { "name", "Alice" }, { "age", 30 }, { "city", "NYC" } });

        var doc = await col.Find(Builders<BsonDocument>.Filter.Empty)
            .Project(Builders<BsonDocument>.Projection.Include("name").Exclude("_id"))
            .FirstOrDefaultAsync();

        Assert.True(doc.Contains("name"));
        Assert.False(doc.Contains("age"));
        Assert.False(doc.Contains("city"));
        Assert.False(doc.Contains("_id"));
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Find_Projection_ExcludeFields()
    {
        var col = _fixture.GetCollection<BsonDocument>("findadvanced_1");
        await col.InsertOneAsync(new BsonDocument { { "name", "Alice" }, { "age", 30 }, { "city", "NYC" } });

        var doc = await col.Find(Builders<BsonDocument>.Filter.Empty)
            .Project(Builders<BsonDocument>.Projection.Exclude("age"))
            .FirstOrDefaultAsync();

        Assert.True(doc.Contains("name"));
        Assert.False(doc.Contains("age"));
        Assert.True(doc.Contains("city"));
    }

    #endregion
}
