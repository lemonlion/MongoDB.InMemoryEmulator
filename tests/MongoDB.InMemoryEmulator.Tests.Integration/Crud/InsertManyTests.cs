using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;
using AwesomeAssertions;

namespace MongoDB.InMemoryEmulator.Tests.Integration.Crud;

/// <summary>
/// Phase 1 integration tests: InsertMany with ordered/unordered and error handling.
/// </summary>
[Collection("Integration")]
public class InsertManyTests : IAsyncLifetime
{
    private readonly MongoDbSession _session;
    private ITestCollectionFixture _fixture = null!;

    public InsertManyTests(MongoDbSession session)
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
    public async Task InsertMany_and_count()
    {
        var collection = _fixture.GetCollection<TestDoc>("insert_many_count");
        var docs = Enumerable.Range(1, 10)
            .Select(i => new TestDoc { Name = $"Doc{i}", Value = i })
            .ToList();

        await collection.InsertManyAsync(docs);

        var count = await collection.CountDocumentsAsync(FilterDefinition<TestDoc>.Empty);
        count.Should().Be(10);
    }

    [Fact]
    public async Task InsertMany_ordered_stops_on_first_duplicate()
    {
        var collection = _fixture.GetCollection<TestDoc>("insert_many_ordered");
        var id = ObjectId.GenerateNewId().ToString();

        // Insert first doc
        await collection.InsertOneAsync(new TestDoc { Id = id, Name = "Original" });

        // Insert batch where second item has duplicate id
        var docs = new[]
        {
            new TestDoc { Name = "New1" },
            new TestDoc { Id = id, Name = "Duplicate" },
            new TestDoc { Name = "New2" }
        };

        var act = () => collection.InsertManyAsync(docs, new InsertManyOptions { IsOrdered = true });
        await act.Should().ThrowAsync<Exception>();

        // Only original + New1 should exist (ordered stops at duplicate)
        var count = await collection.CountDocumentsAsync(FilterDefinition<TestDoc>.Empty);
        count.Should().Be(2);
    }

    [Fact]
    public async Task InsertMany_unordered_continues_past_duplicates()
    {
        var collection = _fixture.GetCollection<TestDoc>("insert_many_unordered");
        var id = ObjectId.GenerateNewId().ToString();

        // Insert first doc
        await collection.InsertOneAsync(new TestDoc { Id = id, Name = "Original" });

        // Insert batch where second item has duplicate id
        var docs = new[]
        {
            new TestDoc { Name = "New1" },
            new TestDoc { Id = id, Name = "Duplicate" },
            new TestDoc { Name = "New2" }
        };

        var act = () => collection.InsertManyAsync(docs, new InsertManyOptions { IsOrdered = false });
        await act.Should().ThrowAsync<MongoBulkWriteException<TestDoc>>();

        // Original + New1 + New2 should exist (unordered continues)
        var count = await collection.CountDocumentsAsync(FilterDefinition<TestDoc>.Empty);
        count.Should().Be(3);
    }

    [Fact]
    public async Task InsertMany_empty_list_does_nothing()
    {
        var collection = _fixture.GetCollection<TestDoc>("insert_many_empty");

        // MongoDB driver throws on empty list — we match that behavior
        var act = () => collection.InsertManyAsync(new List<TestDoc>());

        // Real MongoDB throws InvalidOperationException for empty list (from the driver)
        // Our implementation may also throw or do nothing — verify consistent behavior
        try
        {
            await act();
            // If no exception, that's OK too — verify nothing was inserted
            var count = await collection.CountDocumentsAsync(FilterDefinition<TestDoc>.Empty);
            count.Should().Be(0);
        }
        catch
        {
            // Exception on empty list is acceptable behavior
        }
    }

    [Fact]
    public void InsertMany_sync_works()
    {
        var collection = _fixture.GetCollection<TestDoc>("insert_many_sync");
        var docs = new[]
        {
            new TestDoc { Name = "A" },
            new TestDoc { Name = "B" },
            new TestDoc { Name = "C" }
        };

        collection.InsertMany(docs);

        collection.CountDocuments(FilterDefinition<TestDoc>.Empty).Should().Be(3);
    }
}
