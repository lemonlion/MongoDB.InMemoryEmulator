using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;
using AwesomeAssertions;

namespace MongoDB.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Phase 4 integration tests: Views — creation, transparent querying, and aggregation pipeline.
/// </summary>
[Collection("Integration")]
public class ViewTests : IAsyncLifetime
{
    private readonly MongoDbSession _session;
    private ITestCollectionFixture _fixture = null!;

    public ViewTests(MongoDbSession session)
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

    [Fact]
    public async Task CreateView_and_query_transparently()
    {
        // Ref: https://www.mongodb.com/docs/manual/core/views/
        //   "A MongoDB view is a read-only queryable object whose contents are defined by an
        //    aggregation pipeline on other collections or views."
        var db = _fixture.Database;
        var sourceCollection = db.GetCollection<BsonDocument>("view_source");

        await sourceCollection.InsertManyAsync(new[]
        {
            new BsonDocument { { "name", "Alice" }, { "age", 30 }, { "dept", "Eng" } },
            new BsonDocument { { "name", "Bob" }, { "age", 25 }, { "dept", "Eng" } },
            new BsonDocument { { "name", "Charlie" }, { "age", 35 }, { "dept", "HR" } },
        });

        // Create a view that filters to Engineering department
        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$match", new BsonDocument("dept", "Eng")));

        db.CreateView("eng_view", "view_source", pipeline);

        // Query the view transparently
        var viewCollection = db.GetCollection<BsonDocument>("eng_view");
        var results = await viewCollection.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Equal("Eng", r["dept"].AsString));
    }

    [Fact]
    public async Task View_with_project_pipeline()
    {
        var db = _fixture.Database;
        var source = db.GetCollection<BsonDocument>("view_project_src");

        await source.InsertManyAsync(new[]
        {
            new BsonDocument { { "first", "Alice" }, { "last", "Smith" }, { "salary", 100000 } },
            new BsonDocument { { "first", "Bob" }, { "last", "Jones" }, { "salary", 80000 } },
        });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$project", new BsonDocument
            {
                { "fullName", new BsonDocument("$concat", new BsonArray { "$first", " ", "$last" }) },
                { "_id", 0 }
            }));

        db.CreateView("name_view", "view_project_src", pipeline);

        var viewCollection = db.GetCollection<BsonDocument>("name_view");
        var results = await viewCollection.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r["fullName"].AsString == "Alice Smith");
        Assert.Contains(results, r => r["fullName"].AsString == "Bob Jones");
    }

    [Fact]
    public async Task View_with_filter_on_top()
    {
        // Querying a view with an additional filter
        var db = _fixture.Database;
        var source = db.GetCollection<BsonDocument>("view_filter_src");

        await source.InsertManyAsync(new[]
        {
            new BsonDocument { { "name", "Alice" }, { "score", 90 } },
            new BsonDocument { { "name", "Bob" }, { "score", 60 } },
            new BsonDocument { { "name", "Charlie" }, { "score", 85 } },
        });

        // View: only scores >= 70
        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$match", new BsonDocument("score", new BsonDocument("$gte", 70))));

        db.CreateView("high_score_view", "view_filter_src", pipeline);

        var viewCollection = db.GetCollection<BsonDocument>("high_score_view");

        // Additional filter on top of the view
        var results = await viewCollection.Find(
            Builders<BsonDocument>.Filter.Gt("score", 80)).ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.True(r["score"].AsInt32 > 80));
    }

    [Fact]
    public async Task View_reflects_source_changes()
    {
        // Views are live — data inserted into source appears in view
        var db = _fixture.Database;
        var source = db.GetCollection<BsonDocument>("view_live_src");
        await source.InsertOneAsync(new BsonDocument { { "name", "Initial" }, { "active", true } });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$match", new BsonDocument("active", true)));
        db.CreateView("live_view", "view_live_src", pipeline);

        var viewColl = db.GetCollection<BsonDocument>("live_view");
        var before = await viewColl.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();
        Assert.Single(before);

        // Add more data to source
        await source.InsertOneAsync(new BsonDocument { { "name", "Added" }, { "active", true } });
        await source.InsertOneAsync(new BsonDocument { { "name", "Inactive" }, { "active", false } });

        var after = await viewColl.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();
        Assert.Equal(2, after.Count); // Only active ones
    }

    [Fact]
    public async Task CreateViewAsync_works()
    {
        var db = _fixture.Database;
        var source = db.GetCollection<BsonDocument>("view_async_src");
        await source.InsertOneAsync(new BsonDocument { { "x", 1 } });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(Array.Empty<BsonDocument>());
        await db.CreateViewAsync("async_view", "view_async_src", pipeline);

        var viewColl = db.GetCollection<BsonDocument>("async_view");
        var results = await viewColl.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();
        Assert.Single(results);
    }

    [Fact]
    public async Task ListCollections_includes_views()
    {
        var db = _fixture.Database;
        db.GetCollection<BsonDocument>("lc_src"); // ensure source exists
        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(Array.Empty<BsonDocument>());
        db.CreateView("lc_view", "lc_src", pipeline);

        var cursor = db.ListCollections();
        var collections = cursor.ToList();

        var view = collections.FirstOrDefault(c => c["name"].AsString == "lc_view");
        Assert.NotNull(view);
        Assert.Equal("view", view["type"].AsString);
    }
}
