using MongoDB.Driver;
using Xunit;
using AwesomeAssertions;

namespace MongoDB.InMemoryEmulator.Tests.Integration.Crud;

/// <summary>
/// Phase 1 integration tests: WithReadConcern, WithWriteConcern, WithReadPreference.
/// </summary>
[Collection("Integration")]
public class WithConcernTests : IAsyncLifetime
{
    private readonly MongoDbSession _session;
    private ITestCollectionFixture _fixture = null!;

    public WithConcernTests(MongoDbSession session)
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
    public async Task WithReadConcern_returns_new_collection_sharing_data()
    {
        var collection = _fixture.GetCollection<TestDoc>("with_read_concern");
        await collection.InsertOneAsync(new TestDoc { Name = "Shared" });

        var withConcern = collection.WithReadConcern(ReadConcern.Majority);

        withConcern.Should().NotBeSameAs(collection);
        withConcern.Settings.ReadConcern.Should().Be(ReadConcern.Majority);

        // Data is shared
        var count = await withConcern.CountDocumentsAsync(FilterDefinition<TestDoc>.Empty);
        count.Should().Be(1);
    }

    [Fact]
    public async Task WithWriteConcern_returns_new_collection_sharing_data()
    {
        var collection = _fixture.GetCollection<TestDoc>("with_write_concern");
        await collection.InsertOneAsync(new TestDoc { Name = "Shared" });

        var withConcern = collection.WithWriteConcern(WriteConcern.WMajority);

        withConcern.Should().NotBeSameAs(collection);
        withConcern.Settings.WriteConcern.Should().Be(WriteConcern.WMajority);

        // Data is shared — write through new collection, read through original
        await withConcern.InsertOneAsync(new TestDoc { Name = "New" });
        var count = await collection.CountDocumentsAsync(FilterDefinition<TestDoc>.Empty);
        count.Should().Be(2);
    }

    [Fact]
    public async Task WithReadPreference_returns_new_collection_sharing_data()
    {
        var collection = _fixture.GetCollection<TestDoc>("with_read_pref");
        await collection.InsertOneAsync(new TestDoc { Name = "Shared" });

        var withPref = collection.WithReadPreference(ReadPreference.SecondaryPreferred);

        withPref.Should().NotBeSameAs(collection);
        withPref.Settings.ReadPreference.Should().Be(ReadPreference.SecondaryPreferred);

        var count = await withPref.CountDocumentsAsync(FilterDefinition<TestDoc>.Empty);
        count.Should().Be(1);
    }
}
