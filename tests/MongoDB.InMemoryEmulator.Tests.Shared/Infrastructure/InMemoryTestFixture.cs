using MongoDB.Driver;

namespace MongoDB.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Test fixture for the in-memory emulator. Creates a fresh client and database per fixture.
/// </summary>
public class InMemoryTestFixture : ITestCollectionFixture
{
    private readonly IMongoClient _client;
    private readonly IMongoDatabase _database;

    public InMemoryTestFixture()
    {
        _client = new InMemoryMongoClient();
        _database = _client.GetDatabase($"test_{Guid.NewGuid():N}");
    }

    public TestTarget Target => TestTarget.InMemory;
    public bool IsRealMongo => false;
    public IMongoClient Client => _client;
    public IMongoDatabase Database => _database;

    public IMongoCollection<T> GetCollection<T>(string name) =>
        _database.GetCollection<T>(name);

    public Task ResetAsync()
    {
        // In-memory: drop and re-create database for clean state
        _client.DropDatabase(_database.DatabaseNamespace.DatabaseName);
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
