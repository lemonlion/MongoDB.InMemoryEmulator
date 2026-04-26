using System.Collections.Concurrent;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace MongoDB.InMemoryEmulator;

/// <summary>
/// In-memory implementation of <see cref="IMongoDatabase"/>.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/reference/command/
///   "Database commands provide administrative and diagnostic functionality."
///
/// Shared backing store: GetCollection&lt;T&gt;(name) keys storage by name only (not by typeof(T)).
/// Production code commonly does db.GetCollection&lt;Order&gt;("orders") and
/// db.GetCollection&lt;BsonDocument&gt;("orders") against the same data — both must
/// read/write the same DocumentStore.
/// </remarks>
public class InMemoryMongoDatabase : IMongoDatabase
{
    private readonly ConcurrentDictionary<string, DocumentStore> _stores = new();
    private readonly ConcurrentDictionary<string, bool> _explicitlyCreated = new();

    internal InMemoryMongoDatabase(DatabaseNamespace databaseNamespace, IMongoClient client, MongoDatabaseSettings? settings = null)
    {
        DatabaseNamespace = databaseNamespace ?? throw new ArgumentNullException(nameof(databaseNamespace));
        Client = client ?? throw new ArgumentNullException(nameof(client));
        Settings = settings ?? new MongoDatabaseSettings();
    }

    public DatabaseNamespace DatabaseNamespace { get; }
    public IMongoClient Client { get; }
    public MongoDatabaseSettings Settings { get; }

    internal ConcurrentDictionary<string, DocumentStore> Stores => _stores;

    /// <summary>
    /// Registers an external DocumentStore for a given collection name.
    /// Used when creating standalone InMemoryMongoCollection instances.
    /// </summary>
    internal void RegisterExternalStore(string collectionName, DocumentStore store)
    {
        _stores.TryAdd(collectionName, store);
    }

    internal DocumentStore GetOrCreateStore(string collectionName)
    {
        return _stores.GetOrAdd(collectionName, _ => new DocumentStore());
    }

    internal DocumentStore? GetStore(string collectionName)
    {
        return _stores.TryGetValue(collectionName, out var store) ? store : null;
    }

    #region GetCollection

    public IMongoCollection<TDocument> GetCollection<TDocument>(string name, MongoCollectionSettings? settings = null)
    {
        var store = GetOrCreateStore(name);
        var ns = new CollectionNamespace(DatabaseNamespace, name);
        return new InMemoryMongoCollection<TDocument>(ns, this, store, settings);
    }

    #endregion

    #region CreateCollection

    // Ref: https://www.mongodb.com/docs/manual/reference/command/create/
    //   "Explicitly creates a collection or view."

    public void CreateCollection(string name, CreateCollectionOptions? options = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (!_explicitlyCreated.TryAdd(name, true) && _explicitlyCreated.ContainsKey(name))
        {
            // Collection already explicitly created — OK per MongoDB behavior (no error for already existing)
        }

        GetOrCreateStore(name);
    }

    public void CreateCollection(IClientSessionHandle session, string name, CreateCollectionOptions? options = null, CancellationToken cancellationToken = default)
        => CreateCollection(name, options, cancellationToken);

    public Task CreateCollectionAsync(string name, CreateCollectionOptions? options = null, CancellationToken cancellationToken = default)
    {
        CreateCollection(name, options, cancellationToken);
        return Task.CompletedTask;
    }

    public Task CreateCollectionAsync(IClientSessionHandle session, string name, CreateCollectionOptions? options = null, CancellationToken cancellationToken = default)
    {
        CreateCollection(name, options, cancellationToken);
        return Task.CompletedTask;
    }

    #endregion

    #region DropCollection

    // Ref: https://www.mongodb.com/docs/manual/reference/command/drop/
    //   "Removes an entire collection from a database."

    public void DropCollection(string name, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        _stores.TryRemove(name, out _);
        _explicitlyCreated.TryRemove(name, out _);
    }

    public void DropCollection(IClientSessionHandle session, string name, CancellationToken cancellationToken = default)
        => DropCollection(name, cancellationToken);

    public void DropCollection(string name, DropCollectionOptions options, CancellationToken cancellationToken = default)
        => DropCollection(name, cancellationToken);

    public void DropCollection(IClientSessionHandle session, string name, DropCollectionOptions options, CancellationToken cancellationToken = default)
        => DropCollection(name, cancellationToken);

    public Task DropCollectionAsync(string name, CancellationToken cancellationToken = default)
    {
        DropCollection(name, cancellationToken);
        return Task.CompletedTask;
    }

    public Task DropCollectionAsync(IClientSessionHandle session, string name, CancellationToken cancellationToken = default)
    {
        DropCollection(name, cancellationToken);
        return Task.CompletedTask;
    }

    public Task DropCollectionAsync(string name, DropCollectionOptions options, CancellationToken cancellationToken = default)
    {
        DropCollection(name, cancellationToken);
        return Task.CompletedTask;
    }

    public Task DropCollectionAsync(IClientSessionHandle session, string name, DropCollectionOptions options, CancellationToken cancellationToken = default)
    {
        DropCollection(name, cancellationToken);
        return Task.CompletedTask;
    }

    #endregion

    #region ListCollections / ListCollectionNames

    // Ref: https://www.mongodb.com/docs/manual/reference/command/listCollections/
    //   "Retrieve information about collections and views in a database."

    public IAsyncCursor<BsonDocument> ListCollections(ListCollectionsOptions? options = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var docs = _stores.Keys.Select(name => new BsonDocument
        {
            { "name", name },
            { "type", "collection" },
            { "options", new BsonDocument() },
            { "info", new BsonDocument { { "readOnly", false }, { "uuid", Guid.NewGuid().ToString() } } }
        }).ToList();

        if (options?.Filter != null)
        {
            var rendered = options.Filter.Render(new RenderArgs<BsonDocument>(
                BsonSerializer.LookupSerializer<BsonDocument>(),
                BsonSerializer.SerializerRegistry));
            docs = docs.Where(d => BsonFilterEvaluator.Matches(d, rendered)).ToList();
        }

        return new InMemoryAsyncCursor<BsonDocument>(docs);
    }

    public IAsyncCursor<BsonDocument> ListCollections(IClientSessionHandle session, ListCollectionsOptions? options = null, CancellationToken cancellationToken = default)
        => ListCollections(options, cancellationToken);

    public Task<IAsyncCursor<BsonDocument>> ListCollectionsAsync(ListCollectionsOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(ListCollections(options, cancellationToken));

    public Task<IAsyncCursor<BsonDocument>> ListCollectionsAsync(IClientSessionHandle session, ListCollectionsOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(ListCollections(options, cancellationToken));

    public IAsyncCursor<string> ListCollectionNames(ListCollectionNamesOptions? options = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var names = _stores.Keys.ToList();

        if (options?.Filter != null)
        {
            var rendered = options.Filter.Render(new RenderArgs<BsonDocument>(
                BsonSerializer.LookupSerializer<BsonDocument>(),
                BsonSerializer.SerializerRegistry));
            var docs = names.Select(n => new BsonDocument("name", n));
            names = docs.Where(d => BsonFilterEvaluator.Matches(d, rendered)).Select(d => d["name"].AsString).ToList();
        }

        return new InMemoryAsyncCursor<string>(names);
    }

    public IAsyncCursor<string> ListCollectionNames(IClientSessionHandle session, ListCollectionNamesOptions? options = null, CancellationToken cancellationToken = default)
        => ListCollectionNames(options, cancellationToken);

    public Task<IAsyncCursor<string>> ListCollectionNamesAsync(ListCollectionNamesOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(ListCollectionNames(options, cancellationToken));

    public Task<IAsyncCursor<string>> ListCollectionNamesAsync(IClientSessionHandle session, ListCollectionNamesOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(ListCollectionNames(options, cancellationToken));

    #endregion

    #region RenameCollection

    // Ref: https://www.mongodb.com/docs/manual/reference/command/renameCollection/
    //   "Changes the name of an existing collection."

    public void RenameCollection(string oldName, string newName, RenameCollectionOptions? options = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (_stores.TryRemove(oldName, out var store))
        {
            if (!_stores.TryAdd(newName, store))
                throw MongoErrors.NamespaceExists($"{DatabaseNamespace.DatabaseName}.{newName}");

            _explicitlyCreated.TryRemove(oldName, out _);
            _explicitlyCreated.TryAdd(newName, true);
        }
        else
        {
            throw MongoErrors.NamespaceNotFound($"{DatabaseNamespace.DatabaseName}.{oldName}");
        }
    }

    public void RenameCollection(IClientSessionHandle session, string oldName, string newName, RenameCollectionOptions? options = null, CancellationToken cancellationToken = default)
        => RenameCollection(oldName, newName, options, cancellationToken);

    public Task RenameCollectionAsync(string oldName, string newName, RenameCollectionOptions? options = null, CancellationToken cancellationToken = default)
    {
        RenameCollection(oldName, newName, options, cancellationToken);
        return Task.CompletedTask;
    }

    public Task RenameCollectionAsync(IClientSessionHandle session, string oldName, string newName, RenameCollectionOptions? options = null, CancellationToken cancellationToken = default)
    {
        RenameCollection(oldName, newName, options, cancellationToken);
        return Task.CompletedTask;
    }

    #endregion

    #region RunCommand (stub)

    public TResult RunCommand<TResult>(Command<TResult> command, ReadPreference? readPreference = null, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("RunCommand is not yet implemented. Coming in Phase 4.");
    }

    public TResult RunCommand<TResult>(IClientSessionHandle session, Command<TResult> command, ReadPreference? readPreference = null, CancellationToken cancellationToken = default)
        => RunCommand(command, readPreference, cancellationToken);

    public Task<TResult> RunCommandAsync<TResult>(Command<TResult> command, ReadPreference? readPreference = null, CancellationToken cancellationToken = default)
        => Task.FromResult(RunCommand(command, readPreference, cancellationToken));

    public Task<TResult> RunCommandAsync<TResult>(IClientSessionHandle session, Command<TResult> command, ReadPreference? readPreference = null, CancellationToken cancellationToken = default)
        => Task.FromResult(RunCommand(command, readPreference, cancellationToken));

    #endregion

    #region Aggregate

    // Ref: https://www.mongodb.com/docs/manual/reference/method/db.aggregate/
    //   "Runs an aggregation pipeline on the database without a specific collection."
    public IAsyncCursor<TResult> Aggregate<TResult>(PipelineDefinition<NoPipelineInput, TResult> pipeline, AggregateOptions? options = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var registry = BsonSerializer.SerializerRegistry;
        var serializer = BsonSerializer.LookupSerializer<NoPipelineInput>();
        var rendered = pipeline.Render(new RenderArgs<NoPipelineInput>(serializer, registry));
        var stages = rendered.Documents.ToList();

        var client = Client as InMemoryMongoClient;
        var context = new AggregationContext(this, client)
        {
            CollectionNamespace = DatabaseNamespace.DatabaseName,
            DocumentCount = 0
        };

        // Database-level aggregation starts with empty input
        var input = Enumerable.Empty<BsonDocument>();
        var results = AggregationPipelineExecutor.Execute(input, stages, context).ToList();
        var deserialized = results.Select(bson => BsonSerializer.Deserialize<TResult>(bson)).ToList();
        return new InMemoryAsyncCursor<TResult>(deserialized);
    }

    public IAsyncCursor<TResult> Aggregate<TResult>(IClientSessionHandle session, PipelineDefinition<NoPipelineInput, TResult> pipeline, AggregateOptions? options = null, CancellationToken cancellationToken = default)
        => Aggregate(pipeline, options, cancellationToken);

    public Task<IAsyncCursor<TResult>> AggregateAsync<TResult>(PipelineDefinition<NoPipelineInput, TResult> pipeline, AggregateOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(Aggregate(pipeline, options, cancellationToken));

    public Task<IAsyncCursor<TResult>> AggregateAsync<TResult>(IClientSessionHandle session, PipelineDefinition<NoPipelineInput, TResult> pipeline, AggregateOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(Aggregate(pipeline, options, cancellationToken));

    public void AggregateToCollection<TResult>(PipelineDefinition<NoPipelineInput, TResult> pipeline, AggregateOptions? options = null, CancellationToken cancellationToken = default)
    {
        Aggregate(pipeline, options, cancellationToken);
    }

    public void AggregateToCollection<TResult>(IClientSessionHandle session, PipelineDefinition<NoPipelineInput, TResult> pipeline, AggregateOptions? options = null, CancellationToken cancellationToken = default)
        => AggregateToCollection(pipeline, options, cancellationToken);

    public Task AggregateToCollectionAsync<TResult>(PipelineDefinition<NoPipelineInput, TResult> pipeline, AggregateOptions? options = null, CancellationToken cancellationToken = default)
    {
        AggregateToCollection(pipeline, options, cancellationToken);
        return Task.CompletedTask;
    }

    public Task AggregateToCollectionAsync<TResult>(IClientSessionHandle session, PipelineDefinition<NoPipelineInput, TResult> pipeline, AggregateOptions? options = null, CancellationToken cancellationToken = default)
    {
        AggregateToCollection(pipeline, options, cancellationToken);
        return Task.CompletedTask;
    }

    #endregion

    #region CreateView (stub)

    public void CreateView<TDocument, TResult>(string viewName, string viewOn, PipelineDefinition<TDocument, TResult> pipeline, CreateViewOptions<TDocument>? options = null, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("CreateView is not yet implemented. Coming in Phase 4.");
    }

    public void CreateView<TDocument, TResult>(IClientSessionHandle session, string viewName, string viewOn, PipelineDefinition<TDocument, TResult> pipeline, CreateViewOptions<TDocument>? options = null, CancellationToken cancellationToken = default)
        => CreateView(viewName, viewOn, pipeline, options, cancellationToken);

    public Task CreateViewAsync<TDocument, TResult>(string viewName, string viewOn, PipelineDefinition<TDocument, TResult> pipeline, CreateViewOptions<TDocument>? options = null, CancellationToken cancellationToken = default)
    {
        CreateView(viewName, viewOn, pipeline, options, cancellationToken);
        return Task.CompletedTask;
    }

    public Task CreateViewAsync<TDocument, TResult>(IClientSessionHandle session, string viewName, string viewOn, PipelineDefinition<TDocument, TResult> pipeline, CreateViewOptions<TDocument>? options = null, CancellationToken cancellationToken = default)
    {
        CreateView(viewName, viewOn, pipeline, options, cancellationToken);
        return Task.CompletedTask;
    }

    #endregion

    #region Watch (stub)

    public IChangeStreamCursor<TResult> Watch<TResult>(PipelineDefinition<ChangeStreamDocument<BsonDocument>, TResult> pipeline, ChangeStreamOptions? options = null, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("Watch is not yet implemented. Coming in Phase 4.");
    }

    public IChangeStreamCursor<TResult> Watch<TResult>(IClientSessionHandle session, PipelineDefinition<ChangeStreamDocument<BsonDocument>, TResult> pipeline, ChangeStreamOptions? options = null, CancellationToken cancellationToken = default)
        => Watch(pipeline, options, cancellationToken);

    public Task<IChangeStreamCursor<TResult>> WatchAsync<TResult>(PipelineDefinition<ChangeStreamDocument<BsonDocument>, TResult> pipeline, ChangeStreamOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(Watch(pipeline, options, cancellationToken));

    public Task<IChangeStreamCursor<TResult>> WatchAsync<TResult>(IClientSessionHandle session, PipelineDefinition<ChangeStreamDocument<BsonDocument>, TResult> pipeline, ChangeStreamOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(Watch(pipeline, options, cancellationToken));

    #endregion

    #region WithReadConcern / WithWriteConcern / WithReadPreference

    public IMongoDatabase WithReadConcern(ReadConcern readConcern)
    {
        var settings = Settings.Clone();
        settings.ReadConcern = readConcern;
        var db = new InMemoryMongoDatabase(DatabaseNamespace, Client, settings);
        // Share stores with the original database
        foreach (var kvp in _stores)
            db._stores.TryAdd(kvp.Key, kvp.Value);
        foreach (var kvp in _explicitlyCreated)
            db._explicitlyCreated.TryAdd(kvp.Key, kvp.Value);
        return db;
    }

    public IMongoDatabase WithWriteConcern(WriteConcern writeConcern)
    {
        var settings = Settings.Clone();
        settings.WriteConcern = writeConcern;
        var db = new InMemoryMongoDatabase(DatabaseNamespace, Client, settings);
        foreach (var kvp in _stores)
            db._stores.TryAdd(kvp.Key, kvp.Value);
        foreach (var kvp in _explicitlyCreated)
            db._explicitlyCreated.TryAdd(kvp.Key, kvp.Value);
        return db;
    }

    public IMongoDatabase WithReadPreference(ReadPreference readPreference)
    {
        var settings = Settings.Clone();
        settings.ReadPreference = readPreference;
        var db = new InMemoryMongoDatabase(DatabaseNamespace, Client, settings);
        foreach (var kvp in _stores)
            db._stores.TryAdd(kvp.Key, kvp.Value);
        foreach (var kvp in _explicitlyCreated)
            db._explicitlyCreated.TryAdd(kvp.Key, kvp.Value);
        return db;
    }

    #endregion
}
