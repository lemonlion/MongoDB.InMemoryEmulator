using MongoDB.Driver;

namespace MongoDB.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// xUnit collection fixture that determines the test target from environment variables
/// and creates an appropriate <see cref="IMongoClient"/>.
/// Shared across all test classes in the <see cref="IntegrationCollection"/>.
/// </summary>
public class MongoDbSession : IDisposable
{
    public TestTarget Target { get; }
    public string? ConnectionString { get; }
    public IMongoClient Client { get; }

    public MongoDbSession()
    {
        var target = Environment.GetEnvironmentVariable("MONGO_TEST_TARGET") ?? "inmemory";
        Target = target.ToLowerInvariant() switch
        {
            "mongodb" => TestTarget.MongoDB,
            "atlas" => TestTarget.MongoDBAtlas,
            _ => TestTarget.InMemory,
        };

        if (Target == TestTarget.InMemory)
        {
            Client = new InMemoryMongoClient();
        }
        else
        {
            ConnectionString = Environment.GetEnvironmentVariable("MONGO_CONNECTION_STRING")
                ?? "mongodb://localhost:27017/?replicaSet=rs0&directConnection=true";
            Client = new MongoClient(ConnectionString);
        }
    }

    public bool IsRealMongo => Target != TestTarget.InMemory;
    public bool IsAtlas => Target == TestTarget.MongoDBAtlas;

    public void Dispose()
    {
        // MongoClient is thread-safe singleton — no disposal needed.
        // InMemoryMongoClient has no OS resources.
    }
}
