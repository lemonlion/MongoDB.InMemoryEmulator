using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;
using AwesomeAssertions;

namespace MongoDB.InMemoryEmulator.Tests.Integration.Database;

/// <summary>
/// Phase 1 integration tests: Database and collection management.
/// </summary>
[Collection("Integration")]
public class DatabaseManagementTests : IAsyncLifetime
{
    private readonly MongoDbSession _session;
    private ITestCollectionFixture _fixture = null!;

    public DatabaseManagementTests(MongoDbSession session)
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
    public async Task GetDatabase_and_GetCollection_creates_hierarchy()
    {
        var db = _fixture.Database;
        var collection = db.GetCollection<TestDoc>("hierarchy_test");

        await collection.InsertOneAsync(new TestDoc { Name = "Hierarchy" });

        var count = await collection.CountDocumentsAsync(FilterDefinition<TestDoc>.Empty);
        count.Should().Be(1);
    }

    [Fact]
    public async Task ListCollectionNames_returns_created_collections()
    {
        var db = _fixture.Database;

        // Insert into collections to create them
        await db.GetCollection<TestDoc>("col_a").InsertOneAsync(new TestDoc { Name = "A" });
        await db.GetCollection<TestDoc>("col_b").InsertOneAsync(new TestDoc { Name = "B" });

        var cursor = await db.ListCollectionNamesAsync();
        var names = await cursor.ToListAsync();

        names.Should().Contain("col_a");
        names.Should().Contain("col_b");
    }

    [Fact]
    public async Task ListCollections_returns_metadata()
    {
        var db = _fixture.Database;
        await db.GetCollection<TestDoc>("meta_test").InsertOneAsync(new TestDoc { Name = "Meta" });

        var cursor = await db.ListCollectionsAsync();
        var collections = await cursor.ToListAsync();

        var metaCol = collections.FirstOrDefault(c => c["name"].AsString == "meta_test");
        metaCol.Should().NotBeNull();
        metaCol!["type"].AsString.Should().Be("collection");
    }

    [Fact]
    public async Task DropCollection_removes_collection_and_data()
    {
        var db = _fixture.Database;
        var collection = db.GetCollection<TestDoc>("to_drop");
        await collection.InsertOneAsync(new TestDoc { Name = "WillBeDropped" });

        await db.DropCollectionAsync("to_drop");

        var cursor = await db.ListCollectionNamesAsync();
        var names = await cursor.ToListAsync();
        names.Should().NotContain("to_drop");
    }

    [Fact]
    public async Task CreateCollection_explicitly_creates_collection()
    {
        var db = _fixture.Database;
        await db.CreateCollectionAsync("explicit_create");

        var cursor = await db.ListCollectionNamesAsync();
        var names = await cursor.ToListAsync();
        names.Should().Contain("explicit_create");
    }

    [Fact]
    public async Task GetCollection_multiple_types_share_backing_store()
    {
        var db = _fixture.Database;

        // Insert via typed collection
        var typed = db.GetCollection<TestDoc>("shared_type_test");
        await typed.InsertOneAsync(new TestDoc { Name = "Shared", Value = 42 });

        // Read via BsonDocument collection
        var raw = db.GetCollection<BsonDocument>("shared_type_test");
        var cursor = await raw.FindAsync(FilterDefinition<BsonDocument>.Empty);
        var results = await cursor.ToListAsync();

        results.Should().HaveCount(1);
        results[0]["Name"].AsString.Should().Be("Shared");
    }
}
