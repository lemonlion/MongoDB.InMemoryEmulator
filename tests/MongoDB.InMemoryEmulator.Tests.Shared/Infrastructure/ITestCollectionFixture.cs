using MongoDB.Driver;

namespace MongoDB.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Common interface for test fixtures providing MongoDB collections.
/// Implementations: <see cref="InMemoryTestFixture"/>, <see cref="MongoDbTestFixture"/>.
/// </summary>
public interface ITestCollectionFixture : IAsyncDisposable
{
    TestTarget Target { get; }
    bool IsRealMongo { get; }
    IMongoClient Client { get; }
    IMongoDatabase Database { get; }

    IMongoCollection<T> GetCollection<T>(string name);

    Task ResetAsync();
}
