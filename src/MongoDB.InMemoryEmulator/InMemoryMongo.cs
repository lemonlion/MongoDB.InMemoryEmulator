using MongoDB.Driver;

namespace MongoDB.InMemoryEmulator;

/// <summary>
/// Static factory for creating in-memory MongoDB instances for testing.
/// Equivalent to InMemoryCosmos.Create() in the Cosmos emulator.
/// </summary>
public static class InMemoryMongo
{
    /// <summary>
    /// Creates an in-memory MongoDB setup with a single collection.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    /// <param name="collectionName">Name of the collection.</param>
    /// <param name="databaseName">Name of the database (defaults to "test").</param>
    /// <returns>An <see cref="InMemoryMongoResult{T}"/> with Client, Database, and Collection.</returns>
    public static InMemoryMongoResult<T> Create<T>(string collectionName = "documents", string databaseName = "test")
    {
        var client = new InMemoryMongoClient();
        var database = client.GetDatabase(databaseName);
        var collection = database.GetCollection<T>(collectionName);
        return new InMemoryMongoResult<T>(client, database, collection);
    }

    /// <summary>
    /// Creates an in-memory MongoDB client with a builder for multi-database / multi-collection setups.
    /// </summary>
    public static InMemoryMongoBuilder Builder() => new();
}

/// <summary>
/// Result of <see cref="InMemoryMongo.Create{T}"/>.
/// </summary>
public sealed class InMemoryMongoResult<T>
{
    public InMemoryMongoResult(IMongoClient client, IMongoDatabase database, IMongoCollection<T> collection)
    {
        Client = client;
        Database = database;
        Collection = collection;
    }

    public IMongoClient Client { get; }
    public IMongoDatabase Database { get; }
    public IMongoCollection<T> Collection { get; }
}

/// <summary>
/// Fluent builder for complex in-memory MongoDB setups.
/// </summary>
public sealed class InMemoryMongoBuilder
{
    private readonly InMemoryMongoClient _client = new();
    private readonly List<Action<InMemoryMongoClient>> _configurations = new();

    /// <summary>
    /// Adds a database with configured collections.
    /// </summary>
    public InMemoryMongoBuilder AddDatabase(string name, Action<DatabaseBuilder> configure)
    {
        _configurations.Add(client =>
        {
            var db = (InMemoryMongoDatabase)client.GetDatabase(name);
            var builder = new DatabaseBuilder(db);
            configure(builder);
        });
        return this;
    }

    /// <summary>
    /// Builds the configured in-memory MongoDB client.
    /// </summary>
    public IMongoClient Build()
    {
        foreach (var config in _configurations)
        {
            config(_client);
        }
        return _client;
    }
}

/// <summary>
/// Helper for configuring databases within <see cref="InMemoryMongoBuilder"/>.
/// </summary>
public sealed class DatabaseBuilder
{
    private readonly InMemoryMongoDatabase _database;

    internal DatabaseBuilder(InMemoryMongoDatabase database) => _database = database;

    /// <summary>
    /// Adds a collection to this database.
    /// </summary>
    public DatabaseBuilder AddCollection<T>(string name)
    {
        _database.GetCollection<T>(name);
        return this;
    }
}
