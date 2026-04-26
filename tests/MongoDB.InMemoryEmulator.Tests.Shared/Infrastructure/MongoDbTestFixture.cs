using MongoDB.Driver;

namespace MongoDB.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Test fixture for real MongoDB (Docker or Atlas). Uses a unique database per fixture
/// for isolation and drops it on dispose.
/// </summary>
public class MongoDbTestFixture : ITestCollectionFixture
{
    private readonly string _databaseName;

    public MongoDbTestFixture(MongoDbSession session)
    {
        _databaseName = $"test_{Guid.NewGuid():N}";
        Client = session.Client;
        Database = Client.GetDatabase(_databaseName);
    }

    public TestTarget Target => TestTarget.MongoDB;
    public bool IsRealMongo => true;
    public IMongoClient Client { get; }
    public IMongoDatabase Database { get; }

    public IMongoCollection<T> GetCollection<T>(string name) =>
        Database.GetCollection<T>(name);

    public async Task ResetAsync()
    {
        await Client.DropDatabaseAsync(_databaseName);
    }

    public async ValueTask DisposeAsync()
    {
        await Client.DropDatabaseAsync(_databaseName);
    }
}
