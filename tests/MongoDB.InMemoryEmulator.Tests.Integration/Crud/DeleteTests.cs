using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;
using AwesomeAssertions;

namespace MongoDB.InMemoryEmulator.Tests.Integration.Crud;

/// <summary>
/// Phase 1 integration tests: DeleteOne and DeleteMany.
/// </summary>
[Collection("Integration")]
public class DeleteTests : IAsyncLifetime
{
    private readonly MongoDbSession _session;
    private ITestCollectionFixture _fixture = null!;

    public DeleteTests(MongoDbSession session)
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
    public async Task DeleteOne_by_id()
    {
        var collection = _fixture.GetCollection<TestDoc>("delete_one_id");
        var doc = new TestDoc { Name = "ToDelete" };
        await collection.InsertOneAsync(doc);

        var filter = Builders<TestDoc>.Filter.Eq(d => d.Id, doc.Id);
        var result = await collection.DeleteOneAsync(filter);

        result.DeletedCount.Should().Be(1);
        (await collection.CountDocumentsAsync(FilterDefinition<TestDoc>.Empty)).Should().Be(0);
    }

    [Fact]
    public async Task DeleteOne_no_match_returns_zero()
    {
        var collection = _fixture.GetCollection<TestDoc>("delete_one_no_match");
        await collection.InsertOneAsync(new TestDoc { Name = "Keep" });

        var filter = Builders<TestDoc>.Filter.Eq(d => d.Name, "NonExistent");
        var result = await collection.DeleteOneAsync(filter);

        result.DeletedCount.Should().Be(0);
        (await collection.CountDocumentsAsync(FilterDefinition<TestDoc>.Empty)).Should().Be(1);
    }

    [Fact]
    public async Task DeleteOne_deletes_only_first_match()
    {
        var collection = _fixture.GetCollection<TestDoc>("delete_one_first");
        await collection.InsertManyAsync(
        [
            new TestDoc { Name = "Same", Value = 1 },
            new TestDoc { Name = "Same", Value = 2 },
            new TestDoc { Name = "Same", Value = 3 }
        ]);

        var filter = Builders<TestDoc>.Filter.Eq(d => d.Name, "Same");
        var result = await collection.DeleteOneAsync(filter);

        result.DeletedCount.Should().Be(1);
        (await collection.CountDocumentsAsync(FilterDefinition<TestDoc>.Empty)).Should().Be(2);
    }

    [Fact]
    public async Task DeleteMany_removes_all_matching()
    {
        var collection = _fixture.GetCollection<TestDoc>("delete_many");
        await collection.InsertManyAsync(
        [
            new TestDoc { Name = "A", IsActive = true },
            new TestDoc { Name = "B", IsActive = false },
            new TestDoc { Name = "C", IsActive = true },
            new TestDoc { Name = "D", IsActive = false }
        ]);

        var filter = Builders<TestDoc>.Filter.Eq(d => d.IsActive, true);
        var result = await collection.DeleteManyAsync(filter);

        result.DeletedCount.Should().Be(2);
        (await collection.CountDocumentsAsync(FilterDefinition<TestDoc>.Empty)).Should().Be(2);
    }

    [Fact]
    public async Task DeleteMany_with_empty_filter_removes_all()
    {
        var collection = _fixture.GetCollection<TestDoc>("delete_many_all");
        await collection.InsertManyAsync(
        [
            new TestDoc { Name = "A" },
            new TestDoc { Name = "B" },
            new TestDoc { Name = "C" }
        ]);

        var result = await collection.DeleteManyAsync(FilterDefinition<TestDoc>.Empty);

        result.DeletedCount.Should().Be(3);
        (await collection.CountDocumentsAsync(FilterDefinition<TestDoc>.Empty)).Should().Be(0);
    }
}
