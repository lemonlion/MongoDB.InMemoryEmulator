using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;
using AwesomeAssertions;

namespace MongoDB.InMemoryEmulator.Tests.Integration.Database;

/// <summary>
/// Phase 1 integration tests: Client-level operations (ListDatabases, DropDatabase).
/// </summary>
[Collection("Integration")]
public class ClientTests : IAsyncLifetime
{
    private readonly MongoDbSession _session;
    private ITestCollectionFixture _fixture = null!;

    public ClientTests(MongoDbSession session)
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
    public void GetDatabase_returns_database_instance()
    {
        var db = _fixture.Client.GetDatabase("test_db");
        db.Should().NotBeNull();
        db.DatabaseNamespace.DatabaseName.Should().Be("test_db");
    }

    [Fact]
    public void Client_settings_are_accessible()
    {
        var settings = _fixture.Client.Settings;
        settings.Should().NotBeNull();
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    public async Task ListDatabaseNames_includes_databases_with_data()
    {
        // This test is InMemoryOnly because a real MongoDB instance may have other databases
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("listdb_test");
        var collection = db.GetCollection<TestDoc>("data");
        await collection.InsertOneAsync(new TestDoc { Name = "Data" });

        var cursor = await client.ListDatabaseNamesAsync();
        var names = await cursor.ToListAsync();

        names.Should().Contain("listdb_test");
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    public async Task DropDatabase_removes_database()
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("drop_db_test");
        await db.GetCollection<TestDoc>("data").InsertOneAsync(new TestDoc { Name = "WillBeDropped" });

        await client.DropDatabaseAsync("drop_db_test");

        var cursor = await client.ListDatabaseNamesAsync();
        var names = await cursor.ToListAsync();
        names.Should().NotContain("drop_db_test");
    }
}
