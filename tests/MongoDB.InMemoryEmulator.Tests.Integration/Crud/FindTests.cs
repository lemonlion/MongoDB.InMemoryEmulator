using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;
using AwesomeAssertions;

namespace MongoDB.InMemoryEmulator.Tests.Integration.Crud;

/// <summary>
/// Phase 1 integration tests: Find with filters and cursor behavior.
/// </summary>
[Collection("Integration")]
public class FindTests : IAsyncLifetime
{
    private readonly MongoDbSession _session;
    private ITestCollectionFixture _fixture = null!;

    public FindTests(MongoDbSession session)
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
    public async Task Find_empty_collection_returns_empty()
    {
        var collection = _fixture.GetCollection<TestDoc>("find_empty");

        var cursor = await collection.FindAsync(FilterDefinition<TestDoc>.Empty);
        var results = await cursor.ToListAsync();

        results.Should().BeEmpty();
    }

    [Fact]
    public async Task Find_by_id_filter()
    {
        var collection = _fixture.GetCollection<TestDoc>("find_by_id");
        var doc1 = new TestDoc { Name = "Alpha" };
        var doc2 = new TestDoc { Name = "Beta" };

        await collection.InsertManyAsync([doc1, doc2]);

        var filter = Builders<TestDoc>.Filter.Eq(d => d.Id, doc1.Id);
        var cursor = await collection.FindAsync(filter);
        var results = await cursor.ToListAsync();

        results.Should().HaveCount(1);
        results[0].Name.Should().Be("Alpha");
    }

    [Fact]
    public async Task Find_by_equality_filter()
    {
        var collection = _fixture.GetCollection<TestDoc>("find_eq_filter");
        await collection.InsertManyAsync(
        [
            new TestDoc { Name = "Alice", Value = 1 },
            new TestDoc { Name = "Bob", Value = 2 },
            new TestDoc { Name = "Alice", Value = 3 }
        ]);

        var filter = Builders<TestDoc>.Filter.Eq(d => d.Name, "Alice");
        var cursor = await collection.FindAsync(filter);
        var results = await cursor.ToListAsync();

        results.Should().HaveCount(2);
        results.Should().AllSatisfy(r => r.Name.Should().Be("Alice"));
    }

    [Fact]
    public async Task Find_with_comparison_operators()
    {
        var collection = _fixture.GetCollection<TestDoc>("find_comparison");
        for (int i = 1; i <= 10; i++)
        {
            await collection.InsertOneAsync(new TestDoc { Name = $"Doc{i}", Value = i });
        }

        var filter = Builders<TestDoc>.Filter.Gt(d => d.Value, 7);
        var cursor = await collection.FindAsync(filter);
        var results = await cursor.ToListAsync();

        results.Should().HaveCount(3);
        results.Should().AllSatisfy(r => r.Value.Should().BeGreaterThan(7));
    }

    [Fact]
    public async Task Find_with_and_filter()
    {
        var collection = _fixture.GetCollection<TestDoc>("find_and");
        await collection.InsertManyAsync(
        [
            new TestDoc { Name = "Alice", Value = 10, IsActive = true },
            new TestDoc { Name = "Alice", Value = 20, IsActive = false },
            new TestDoc { Name = "Bob", Value = 10, IsActive = true }
        ]);

        var filter = Builders<TestDoc>.Filter.Eq(d => d.Name, "Alice")
                   & Builders<TestDoc>.Filter.Eq(d => d.IsActive, true);
        var cursor = await collection.FindAsync(filter);
        var results = await cursor.ToListAsync();

        results.Should().HaveCount(1);
        results[0].Name.Should().Be("Alice");
        results[0].Value.Should().Be(10);
    }

    [Fact]
    public async Task Find_with_or_filter()
    {
        var collection = _fixture.GetCollection<TestDoc>("find_or");
        await collection.InsertManyAsync(
        [
            new TestDoc { Name = "Alice", Value = 1 },
            new TestDoc { Name = "Bob", Value = 2 },
            new TestDoc { Name = "Charlie", Value = 3 }
        ]);

        var filter = Builders<TestDoc>.Filter.Eq(d => d.Name, "Alice")
                   | Builders<TestDoc>.Filter.Eq(d => d.Name, "Charlie");
        var cursor = await collection.FindAsync(filter);
        var results = await cursor.ToListAsync();

        results.Should().HaveCount(2);
        results.Select(r => r.Name).Should().BeEquivalentTo(["Alice", "Charlie"]);
    }

    [Fact]
    public async Task Find_with_in_filter()
    {
        var collection = _fixture.GetCollection<TestDoc>("find_in");
        await collection.InsertManyAsync(
        [
            new TestDoc { Name = "Alice", Value = 1 },
            new TestDoc { Name = "Bob", Value = 2 },
            new TestDoc { Name = "Charlie", Value = 3 }
        ]);

        var filter = Builders<TestDoc>.Filter.In(d => d.Value, new[] { 1, 3 });
        var cursor = await collection.FindAsync(filter);
        var results = await cursor.ToListAsync();

        results.Should().HaveCount(2);
        results.Select(r => r.Name).Should().BeEquivalentTo(["Alice", "Charlie"]);
    }

    [Fact]
    public async Task FindSync_works()
    {
        var collection = _fixture.GetCollection<TestDoc>("find_sync");
        await collection.InsertOneAsync(new TestDoc { Name = "SyncFind" });

        var cursor = collection.FindSync(FilterDefinition<TestDoc>.Empty);
        var results = cursor.ToList();

        results.Should().HaveCount(1);
        results[0].Name.Should().Be("SyncFind");
    }

    [Fact]
    public async Task CountDocuments_with_filter()
    {
        var collection = _fixture.GetCollection<TestDoc>("count_filter");
        await collection.InsertManyAsync(
        [
            new TestDoc { Name = "A", IsActive = true },
            new TestDoc { Name = "B", IsActive = true },
            new TestDoc { Name = "C", IsActive = false }
        ]);

        var count = await collection.CountDocumentsAsync(Builders<TestDoc>.Filter.Eq(d => d.IsActive, true));
        count.Should().Be(2);
    }

    [Fact]
    public async Task EstimatedDocumentCount_returns_total()
    {
        var collection = _fixture.GetCollection<TestDoc>("estimated_count");
        await collection.InsertManyAsync(
        [
            new TestDoc { Name = "A" },
            new TestDoc { Name = "B" },
            new TestDoc { Name = "C" }
        ]);

        var count = await collection.EstimatedDocumentCountAsync();
        count.Should().Be(3);
    }
}
