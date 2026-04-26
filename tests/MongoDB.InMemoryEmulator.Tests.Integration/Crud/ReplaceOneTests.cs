using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;
using AwesomeAssertions;

namespace MongoDB.InMemoryEmulator.Tests.Integration.Crud;

/// <summary>
/// Phase 1 integration tests: ReplaceOne.
/// </summary>
[Collection("Integration")]
public class ReplaceOneTests : IAsyncLifetime
{
    private readonly MongoDbSession _session;
    private ITestCollectionFixture _fixture = null!;

    public ReplaceOneTests(MongoDbSession session)
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
    public async Task ReplaceOne_replaces_matched_document()
    {
        var collection = _fixture.GetCollection<TestDoc>("replace_one");
        var doc = new TestDoc { Name = "Original", Value = 1 };
        await collection.InsertOneAsync(doc);

        var filter = Builders<TestDoc>.Filter.Eq(d => d.Id, doc.Id);
        var replacement = new TestDoc { Id = doc.Id, Name = "Replaced", Value = 99 };
        var result = await collection.ReplaceOneAsync(filter, replacement);

        result.MatchedCount.Should().Be(1);
        result.ModifiedCount.Should().Be(1);

        var cursor = await collection.FindAsync(filter);
        var found = await cursor.FirstOrDefaultAsync();
        found!.Name.Should().Be("Replaced");
        found.Value.Should().Be(99);
    }

    [Fact]
    public async Task ReplaceOne_no_match_returns_zero()
    {
        var collection = _fixture.GetCollection<TestDoc>("replace_no_match");
        await collection.InsertOneAsync(new TestDoc { Name = "Keep" });

        var filter = Builders<TestDoc>.Filter.Eq(d => d.Name, "NonExistent");
        var result = await collection.ReplaceOneAsync(filter, new TestDoc { Name = "Replacement" });

        result.MatchedCount.Should().Be(0);
        result.ModifiedCount.Should().Be(0);
    }

    [Fact]
    public async Task ReplaceOne_with_upsert_inserts_when_no_match()
    {
        var collection = _fixture.GetCollection<TestDoc>("replace_upsert");

        var filter = Builders<TestDoc>.Filter.Eq(d => d.Name, "NonExistent");
        var replacement = new TestDoc { Name = "Upserted", Value = 42 };
        var result = await collection.ReplaceOneAsync(filter, replacement, new ReplaceOptions { IsUpsert = true });

        result.MatchedCount.Should().Be(0);
        result.UpsertedId.Should().NotBeNull();

        var count = await collection.CountDocumentsAsync(FilterDefinition<TestDoc>.Empty);
        count.Should().Be(1);
    }

    [Fact]
    public async Task ReplaceOne_with_different_id_throws_immutable_field()
    {
        var collection = _fixture.GetCollection<TestDoc>("replace_diff_id");
        var doc = new TestDoc { Name = "Original" };
        await collection.InsertOneAsync(doc);

        var filter = Builders<TestDoc>.Filter.Eq(d => d.Id, doc.Id);
        var replacement = new TestDoc { Id = ObjectId.GenerateNewId().ToString(), Name = "DiffId" };

        var act = () => collection.ReplaceOneAsync(filter, replacement);
        // Real MongoDB throws MongoWriteException; in-memory throws MongoCommandException.
        // Both are subtypes of MongoServerException.
        await act.Should().ThrowAsync<MongoServerException>();
    }
}
