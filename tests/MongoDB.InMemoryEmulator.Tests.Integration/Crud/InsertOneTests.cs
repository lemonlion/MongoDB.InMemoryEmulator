using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;
using AwesomeAssertions;

namespace MongoDB.InMemoryEmulator.Tests.Integration.Crud;

/// <summary>
/// Phase 1 integration tests: InsertOne and basic Find round-trip.
/// </summary>
[Collection("Integration")]
public class InsertOneTests : IAsyncLifetime
{
    private readonly MongoDbSession _session;
    private ITestCollectionFixture _fixture = null!;

    public InsertOneTests(MongoDbSession session)
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
    public async Task InsertOne_and_FindAsync_round_trip()
    {
        var collection = _fixture.GetCollection<TestDoc>("insert_one_tests");
        var doc = new TestDoc { Name = "Alice", Value = 42, IsActive = true };

        await collection.InsertOneAsync(doc);

        var cursor = await collection.FindAsync(FilterDefinition<TestDoc>.Empty);
        var results = await cursor.ToListAsync();

        results.Should().HaveCount(1);
        results[0].Name.Should().Be("Alice");
        results[0].Value.Should().Be(42);
        results[0].IsActive.Should().BeTrue();
    }

    [Fact]
    public async Task InsertOne_auto_generates_id()
    {
        var collection = _fixture.GetCollection<TestDoc>("insert_id_gen");
        var doc = new TestDoc { Name = "Bob" };

        doc.Id.Should().BeNull();
        await collection.InsertOneAsync(doc);

        // After insert, the Id should be populated
        doc.Id.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task InsertOne_with_explicit_id_preserves_it()
    {
        var collection = _fixture.GetCollection<TestDoc>("insert_explicit_id");
        var id = ObjectId.GenerateNewId().ToString();
        var doc = new TestDoc { Id = id, Name = "Charlie" };

        await collection.InsertOneAsync(doc);

        var cursor = await collection.FindAsync(Builders<TestDoc>.Filter.Eq(d => d.Id, id));
        var found = await cursor.FirstOrDefaultAsync();

        found.Should().NotBeNull();
        found!.Id.Should().Be(id);
        found.Name.Should().Be("Charlie");
    }

    [Fact]
    public async Task InsertOne_duplicate_id_throws_MongoWriteException()
    {
        var collection = _fixture.GetCollection<TestDoc>("insert_dup_id");
        var id = ObjectId.GenerateNewId().ToString();

        await collection.InsertOneAsync(new TestDoc { Id = id, Name = "First" });

        var act = () => collection.InsertOneAsync(new TestDoc { Id = id, Name = "Second" });

        var ex = await act.Should().ThrowAsync<MongoWriteException>();
        ex.Which.WriteError.Category.Should().Be(ServerErrorCategory.DuplicateKey);
        ex.Which.WriteError.Code.Should().Be(11000);
    }

    [Fact]
    public async Task InsertOne_with_nested_document()
    {
        var collection = _fixture.GetCollection<TestDoc>("insert_nested");
        var doc = new TestDoc
        {
            Name = "Dave",
            Value = 10,
            Nested = new NestedDoc { Description = "nested data", Score = 95.5 }
        };

        await collection.InsertOneAsync(doc);

        var cursor = await collection.FindAsync(FilterDefinition<TestDoc>.Empty);
        var found = (await cursor.ToListAsync())[0];

        found.Nested.Should().NotBeNull();
        found.Nested!.Description.Should().Be("nested data");
        found.Nested.Score.Should().Be(95.5);
    }

    [Fact]
    public async Task InsertOne_with_tags_array()
    {
        var collection = _fixture.GetCollection<TestDoc>("insert_tags");
        var doc = new TestDoc { Name = "Eve", Tags = ["tag1", "tag2", "tag3"] };

        await collection.InsertOneAsync(doc);

        var cursor = await collection.FindAsync(FilterDefinition<TestDoc>.Empty);
        var found = (await cursor.ToListAsync())[0];

        found.Tags.Should().BeEquivalentTo(["tag1", "tag2", "tag3"]);
    }

    [Fact]
    public void InsertOne_sync_works()
    {
        var collection = _fixture.GetCollection<TestDoc>("insert_sync");
        var doc = new TestDoc { Name = "Sync" };

        collection.InsertOne(doc);

        var count = collection.CountDocuments(FilterDefinition<TestDoc>.Empty);
        count.Should().Be(1);
    }
}
