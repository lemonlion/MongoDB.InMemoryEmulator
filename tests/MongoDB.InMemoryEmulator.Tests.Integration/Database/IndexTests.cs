using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;
using AwesomeAssertions;

namespace MongoDB.InMemoryEmulator.Tests.Integration.Database;

/// <summary>
/// Phase 1 integration tests: Index manager basic operations.
/// </summary>
[Collection("Integration")]
public class IndexTests : IAsyncLifetime
{
    private readonly MongoDbSession _session;
    private ITestCollectionFixture _fixture = null!;

    public IndexTests(MongoDbSession session)
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
    public async Task Default_id_index_exists()
    {
        var collection = _fixture.GetCollection<TestDoc>("idx_default");
        // Need to create the collection by inserting something
        await collection.InsertOneAsync(new TestDoc { Name = "Idx" });

        var cursor = await collection.Indexes.ListAsync();
        var indexes = await cursor.ToListAsync();

        indexes.Should().ContainSingle(i => i["name"].AsString == "_id_");
    }

    [Fact]
    public async Task CreateOne_creates_named_index()
    {
        var collection = _fixture.GetCollection<TestDoc>("idx_create");
        await collection.InsertOneAsync(new TestDoc { Name = "Idx" });

        var indexName = await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<TestDoc>(
                Builders<TestDoc>.IndexKeys.Ascending(d => d.Name),
                new CreateIndexOptions { Name = "name_1" }));

        indexName.Should().Be("name_1");

        var cursor = await collection.Indexes.ListAsync();
        var indexes = await cursor.ToListAsync();
        indexes.Should().HaveCount(2); // _id_ + name_1
    }

    [Fact]
    public async Task CreateOne_auto_generates_name()
    {
        var collection = _fixture.GetCollection<TestDoc>("idx_autogen");
        await collection.InsertOneAsync(new TestDoc { Name = "Idx" });

        var indexName = await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<TestDoc>(
                Builders<TestDoc>.IndexKeys.Ascending(d => d.Value)));

        indexName.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task DropOne_removes_index()
    {
        var collection = _fixture.GetCollection<TestDoc>("idx_drop");
        await collection.InsertOneAsync(new TestDoc { Name = "Idx" });

        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<TestDoc>(
                Builders<TestDoc>.IndexKeys.Ascending(d => d.Name),
                new CreateIndexOptions { Name = "name_index" }));

        await collection.Indexes.DropOneAsync("name_index");

        var cursor = await collection.Indexes.ListAsync();
        var indexes = await cursor.ToListAsync();
        indexes.Should().ContainSingle(i => i["name"].AsString == "_id_");
    }

    [Fact]
    public async Task DropAll_removes_all_except_id()
    {
        var collection = _fixture.GetCollection<TestDoc>("idx_drop_all");
        await collection.InsertOneAsync(new TestDoc { Name = "Idx" });

        await collection.Indexes.CreateOneAsync(new CreateIndexModel<TestDoc>(
            Builders<TestDoc>.IndexKeys.Ascending(d => d.Name)));
        await collection.Indexes.CreateOneAsync(new CreateIndexModel<TestDoc>(
            Builders<TestDoc>.IndexKeys.Descending(d => d.Value)));

        await collection.Indexes.DropAllAsync();

        var cursor = await collection.Indexes.ListAsync();
        var indexes = await cursor.ToListAsync();
        indexes.Should().ContainSingle(i => i["name"].AsString == "_id_");
    }
}
