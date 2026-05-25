using System.Collections.Concurrent;
using global::MongoDB.Bson;
using global::MongoDB.Bson.Serialization;
using global::MongoDB.Driver;
using global::MongoDB.Driver.Core.Clusters;

namespace InMemoryEmulator.MongoDB;

/// <summary>
/// In-memory implementation of <see cref="IMongoClient"/> for MongoDB.Driver 2.x.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/drivers/csharp/current/
///   IMongoClient is the root interface for interacting with a MongoDB deployment.
///   GetDatabase() creates database handles, which create collection handles.
///
/// V2 differences from V3:
///   - No client-level BulkWrite (MongoDB 8.0+ / Driver 3.x only).
///   - IMongoClient does not extend IDisposable in 2.x.
/// </remarks>
public class InMemoryMongoClient : IMongoClient
{
    private readonly ConcurrentDictionary<string, InMemoryMongoDatabase> _databases = new();
    internal readonly ChangeStreamNotifier ChangeNotifier = new();
    internal readonly CommandEventEmitter? CommandEventEmitter;

    public InMemoryMongoClient(MongoClientSettings? settings = null)
    {
        Settings = settings ?? MongoClientSettings.FromConnectionString("mongodb://localhost:27017");
    }

    /// <summary>
    /// Creates an in-memory client with command event subscriptions.
    /// </summary>
    /// <param name="commandEventSubscribers">
    /// Configurator for subscribing to <see cref="global::MongoDB.Driver.Core.Events.CommandStartedEvent"/>,
    /// <see cref="global::MongoDB.Driver.Core.Events.CommandSucceededEvent"/>, and
    /// <see cref="global::MongoDB.Driver.Core.Events.CommandFailedEvent"/>.
    /// </param>
    /// <param name="settings">Optional client settings.</param>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/drivers/csharp/current/fundamentals/logging/
    ///   "Use MongoClientSettings.ClusterConfigurator to subscribe to command events."
    /// </remarks>
    public InMemoryMongoClient(Action<CommandEventSubscriptionBuilder> commandEventSubscribers, MongoClientSettings? settings = null)
        : this(settings)
    {
        if (commandEventSubscribers != null)
        {
            CommandEventEmitter = new CommandEventEmitter(commandEventSubscribers);
        }
    }

    public MongoClientSettings Settings { get; }

    // Ref: https://www.mongodb.com/docs/drivers/csharp/current/
    //   IMongoClient.Cluster is used for cluster topology monitoring.
    //   In-memory: not supported — SDK components that need Cluster should use our in-memory alternatives.
    public ICluster Cluster => throw new NotSupportedException("Cluster topology is not available in the in-memory emulator. Use InMemoryGridFSBucket for GridFS.");

    #region GetDatabase

    public IMongoDatabase GetDatabase(string name, MongoDatabaseSettings? settings = null)
    {
        return _databases.GetOrAdd(name, n =>
            new InMemoryMongoDatabase(new DatabaseNamespace(n), this, settings));
    }

    #endregion

    #region ListDatabases / DropDatabase

    // Ref: https://www.mongodb.com/docs/manual/reference/command/listDatabases/
    //   "Returns a document that lists all databases and basic statistics about each."

    public IAsyncCursor<BsonDocument> ListDatabases(CancellationToken cancellationToken = default)
        => ListDatabases(options: null, cancellationToken);

    public IAsyncCursor<BsonDocument> ListDatabases(ListDatabasesOptions? options, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        // Ref: https://www.mongodb.com/docs/manual/reference/command/listDatabases/
        //   "By default, listDatabases does not include empty databases."
        var docs = _databases
            .Where(kvp => !kvp.Value.Stores.IsEmpty)
            .Select(kvp => new BsonDocument
            {
                { "name", kvp.Key },
                { "sizeOnDisk", 0 },
                { "empty", false }
            }).ToList();

        return new InMemoryAsyncCursor<BsonDocument>(docs);
    }

    public IAsyncCursor<BsonDocument> ListDatabases(IClientSessionHandle session, CancellationToken cancellationToken = default)
        => ListDatabases(cancellationToken);

    public IAsyncCursor<BsonDocument> ListDatabases(IClientSessionHandle session, ListDatabasesOptions? options, CancellationToken cancellationToken = default)
        => ListDatabases(options, cancellationToken);

    public Task<IAsyncCursor<BsonDocument>> ListDatabasesAsync(CancellationToken cancellationToken = default)
        => Task.FromResult(ListDatabases(cancellationToken));

    public Task<IAsyncCursor<BsonDocument>> ListDatabasesAsync(ListDatabasesOptions? options, CancellationToken cancellationToken = default)
        => Task.FromResult(ListDatabases(options, cancellationToken));

    public Task<IAsyncCursor<BsonDocument>> ListDatabasesAsync(IClientSessionHandle session, CancellationToken cancellationToken = default)
        => Task.FromResult(ListDatabases(cancellationToken));

    public Task<IAsyncCursor<BsonDocument>> ListDatabasesAsync(IClientSessionHandle session, ListDatabasesOptions? options, CancellationToken cancellationToken = default)
        => Task.FromResult(ListDatabases(options, cancellationToken));

    public IAsyncCursor<string> ListDatabaseNames(CancellationToken cancellationToken = default)
        => ListDatabaseNames((ListDatabaseNamesOptions?)null, cancellationToken);

    public IAsyncCursor<string> ListDatabaseNames(ListDatabaseNamesOptions? options, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        // Ref: https://www.mongodb.com/docs/manual/reference/command/listDatabases/
        //   "By default, listDatabases does not include empty databases."
        return new InMemoryAsyncCursor<string>(
            _databases.Where(kvp => !kvp.Value.Stores.IsEmpty).Select(kvp => kvp.Key).ToList());
    }

    public IAsyncCursor<string> ListDatabaseNames(IClientSessionHandle session, CancellationToken cancellationToken = default)
        => ListDatabaseNames(cancellationToken);

    public IAsyncCursor<string> ListDatabaseNames(IClientSessionHandle session, ListDatabaseNamesOptions? options, CancellationToken cancellationToken = default)
        => ListDatabaseNames(options, cancellationToken);

    public Task<IAsyncCursor<string>> ListDatabaseNamesAsync(CancellationToken cancellationToken = default)
        => Task.FromResult(ListDatabaseNames(cancellationToken));

    public Task<IAsyncCursor<string>> ListDatabaseNamesAsync(ListDatabaseNamesOptions? options, CancellationToken cancellationToken = default)
        => Task.FromResult(ListDatabaseNames(options, cancellationToken));

    public Task<IAsyncCursor<string>> ListDatabaseNamesAsync(IClientSessionHandle session, CancellationToken cancellationToken = default)
        => Task.FromResult(ListDatabaseNames(cancellationToken));

    public Task<IAsyncCursor<string>> ListDatabaseNamesAsync(IClientSessionHandle session, ListDatabaseNamesOptions? options, CancellationToken cancellationToken = default)
        => Task.FromResult(ListDatabaseNames(options, cancellationToken));

    // Ref: https://www.mongodb.com/docs/manual/reference/command/dropDatabase/
    //   "The dropDatabase command drops the current database."

    public void DropDatabase(string name, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _databases.TryRemove(name, out _);
    }

    public void DropDatabase(IClientSessionHandle session, string name, CancellationToken cancellationToken = default)
        => DropDatabase(name, cancellationToken);

    public Task DropDatabaseAsync(string name, CancellationToken cancellationToken = default)
    {
        DropDatabase(name, cancellationToken);
        return Task.CompletedTask;
    }

    public Task DropDatabaseAsync(IClientSessionHandle session, string name, CancellationToken cancellationToken = default)
    {
        DropDatabase(name, cancellationToken);
        return Task.CompletedTask;
    }

    #endregion

    #region Sessions

    // Ref: https://www.mongodb.com/docs/manual/reference/method/Mongo.startSession/
    //   "Starts a session for the connection. Returns a ClientSession."
    public IClientSessionHandle StartSession(ClientSessionOptions? options = null, CancellationToken cancellationToken = default)
    {
        return new InMemoryClientSessionHandle(this, options);
    }

    public Task<IClientSessionHandle> StartSessionAsync(ClientSessionOptions? options = null, CancellationToken cancellationToken = default)
    {
        return Task.FromResult<IClientSessionHandle>(new InMemoryClientSessionHandle(this, options));
    }

    #endregion

    #region Watch

    // Ref: https://www.mongodb.com/docs/manual/changeStreams/
    //   "You can open change streams against collections, databases, and deployments."
    public IChangeStreamCursor<TResult> Watch<TResult>(PipelineDefinition<ChangeStreamDocument<BsonDocument>, TResult> pipeline, ChangeStreamOptions? options = null, CancellationToken cancellationToken = default)
    {
        var registry = BsonSerializer.SerializerRegistry;
        var outputSerializer = registry.GetSerializer<TResult>();
        return new InMemoryChangeStreamCursor<TResult>(
            ChangeNotifier,
            databaseFilter: null, // client-level: all databases
            collectionFilter: null,
            options,
            outputSerializer,
            startSequence: ChangeNotifier.CurrentSequence);
    }

    public IChangeStreamCursor<TResult> Watch<TResult>(IClientSessionHandle session, PipelineDefinition<ChangeStreamDocument<BsonDocument>, TResult> pipeline, ChangeStreamOptions? options = null, CancellationToken cancellationToken = default)
        => Watch(pipeline, options, cancellationToken);

    public Task<IChangeStreamCursor<TResult>> WatchAsync<TResult>(PipelineDefinition<ChangeStreamDocument<BsonDocument>, TResult> pipeline, ChangeStreamOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(Watch(pipeline, options, cancellationToken));

    public Task<IChangeStreamCursor<TResult>> WatchAsync<TResult>(IClientSessionHandle session, PipelineDefinition<ChangeStreamDocument<BsonDocument>, TResult> pipeline, ChangeStreamOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(Watch(pipeline, options, cancellationToken));

    #endregion

    #region WithReadConcern / WithWriteConcern / WithReadPreference

    public IMongoClient WithReadConcern(ReadConcern readConcern)
    {
        var newSettings = Settings.Clone();
        newSettings.ReadConcern = readConcern;
        newSettings.Freeze();
        var client = new InMemoryMongoClient(newSettings);
        // Share databases
        foreach (var kvp in _databases)
            client._databases.TryAdd(kvp.Key, kvp.Value);
        return client;
    }

    public IMongoClient WithWriteConcern(WriteConcern writeConcern)
    {
        var newSettings = Settings.Clone();
        newSettings.WriteConcern = writeConcern;
        newSettings.Freeze();
        var client = new InMemoryMongoClient(newSettings);
        foreach (var kvp in _databases)
            client._databases.TryAdd(kvp.Key, kvp.Value);
        return client;
    }

    public IMongoClient WithReadPreference(ReadPreference readPreference)
    {
        var newSettings = Settings.Clone();
        newSettings.ReadPreference = readPreference;
        newSettings.Freeze();
        var client = new InMemoryMongoClient(newSettings);
        foreach (var kvp in _databases)
            client._databases.TryAdd(kvp.Key, kvp.Value);
        return client;
    }

    #endregion

    #region IDisposable

    /// <summary>
    /// Disposes the client. Not required by IMongoClient in 2.x but provided for convenience
    /// and forward-compatibility with code that may call Dispose on the client.
    /// </summary>
    public void Dispose()
    {
        // No-op: in-memory emulator has no external resources to release.
    }

    #endregion
}
