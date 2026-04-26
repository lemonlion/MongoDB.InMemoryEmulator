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

    // View metadata: viewName -> (sourceName, pipeline stages as BsonDocument array)
    private readonly ConcurrentDictionary<string, (string Source, BsonArray Pipeline)> _views = new();

    // Schema validation: collectionName -> (validator, action, level)
    private readonly ConcurrentDictionary<string, SchemaValidation> _validators = new();

    // Time series options: collectionName -> options document
    private readonly ConcurrentDictionary<string, BsonDocument> _timeSeriesOptions = new();

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
        // Ref: https://www.mongodb.com/docs/manual/core/views/
        //   "Queries against a view run the backing pipeline transparently."
        if (_views.TryGetValue(name, out var viewDef))
        {
            // Create a view-backed collection that runs the pipeline against the source
            var sourceStore = GetOrCreateStore(viewDef.Source);
            var ns = new CollectionNamespace(DatabaseNamespace, name);
            return new InMemoryMongoCollection<TDocument>(ns, this, sourceStore, settings,
                viewPipeline: viewDef.Pipeline.Select(s => s.AsBsonDocument).ToList());
        }

        var store = GetOrCreateStore(name);
        var collNs = new CollectionNamespace(DatabaseNamespace, name);
        return new InMemoryMongoCollection<TDocument>(collNs, this, store, settings);
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

        // Ref: https://www.mongodb.com/docs/manual/core/timeseries-collections/
        //   "Time series collections efficiently store time series data."
        if (options?.TimeSeriesOptions != null)
        {
            var ts = options.TimeSeriesOptions;
            var tsDoc = new BsonDocument { { "timeField", ts.TimeField } };
            if (ts.MetaField != null) tsDoc["metaField"] = ts.MetaField;
            if (ts.Granularity.HasValue) tsDoc["granularity"] = ts.Granularity.Value.ToString().ToLowerInvariant();
            _timeSeriesOptions[name] = tsDoc;
        }
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
            { "type", _views.ContainsKey(name) ? "view" : "collection" },
            { "options", new BsonDocument() },
            { "info", new BsonDocument { { "readOnly", _views.ContainsKey(name) }, { "uuid", Guid.NewGuid().ToString() } } }
        }).ToList();

        // Also include views that don't have a store directly
        foreach (var viewName in _views.Keys.Where(v => !_stores.ContainsKey(v)))
        {
            docs.Add(new BsonDocument
            {
                { "name", viewName },
                { "type", "view" },
                { "options", new BsonDocument { { "viewOn", _views[viewName].Source }, { "pipeline", _views[viewName].Pipeline } } },
                { "info", new BsonDocument { { "readOnly", true }, { "uuid", Guid.NewGuid().ToString() } } }
            });
        }

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

    #region RunCommand

    // Ref: https://www.mongodb.com/docs/manual/reference/command/
    //   "Database commands are used to perform administrative and diagnostic tasks."
    public TResult RunCommand<TResult>(Command<TResult> command, ReadPreference? readPreference = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var rendered = command.Render(BsonSerializer.SerializerRegistry);
        var cmdDoc = rendered.Document;
        var resultSerializer = rendered.ResultSerializer;

        var result = DispatchCommand(cmdDoc);

        using var reader = new MongoDB.Bson.IO.BsonDocumentReader(result);
        var ctx = BsonDeserializationContext.CreateRoot(reader);
        return resultSerializer.Deserialize(ctx);
    }

    public TResult RunCommand<TResult>(IClientSessionHandle session, Command<TResult> command, ReadPreference? readPreference = null, CancellationToken cancellationToken = default)
        => RunCommand(command, readPreference, cancellationToken);

    public Task<TResult> RunCommandAsync<TResult>(Command<TResult> command, ReadPreference? readPreference = null, CancellationToken cancellationToken = default)
        => Task.FromResult(RunCommand(command, readPreference, cancellationToken));

    public Task<TResult> RunCommandAsync<TResult>(IClientSessionHandle session, Command<TResult> command, ReadPreference? readPreference = null, CancellationToken cancellationToken = default)
        => Task.FromResult(RunCommand(command, readPreference, cancellationToken));

    // Ref: https://www.mongodb.com/docs/manual/reference/command/ping/
    //   "The ping command is a simple diagnostic command."
    // Ref: https://www.mongodb.com/docs/manual/reference/command/buildInfo/
    //   "Returns a build summary for the current mongod."
    // Ref: https://www.mongodb.com/docs/manual/reference/command/serverStatus/
    //   "Returns a document that provides an overview of the database process state."
    private BsonDocument DispatchCommand(BsonDocument cmd)
    {
        var firstElement = cmd.GetElement(0);
        var commandName = firstElement.Name.ToLowerInvariant();

        return commandName switch
        {
            "ping" => new BsonDocument("ok", 1),
            "buildinfo" => new BsonDocument
            {
                { "ok", 1 },
                { "version", "7.0.0-inmemory" },
                { "gitVersion", "inmemory" },
                { "modules", new BsonArray() },
                { "sysInfo", "InMemoryEmulator" },
                { "versionArray", new BsonArray { 7, 0, 0, 0 } },
                { "bits", 64 },
                { "maxBsonObjectSize", 16 * 1024 * 1024 }
            },
            "serverstatus" => new BsonDocument
            {
                { "ok", 1 },
                { "host", "inmemory" },
                { "version", "7.0.0-inmemory" },
                { "process", "InMemoryEmulator" },
                { "uptime", 1.0 },
                { "connections", new BsonDocument { { "current", 1 }, { "available", 999 }, { "totalCreated", 1 } } },
                { "network", new BsonDocument { { "bytesIn", 0L }, { "bytesOut", 0L }, { "numRequests", 0L } } }
            },
            "hostinfo" => new BsonDocument
            {
                { "ok", 1 },
                { "system", new BsonDocument { { "hostname", Environment.MachineName }, { "cpuArch", "x86_64" } } },
                { "os", new BsonDocument { { "type", "InMemory" }, { "name", "InMemoryEmulator" } } }
            },
            "connectionstatus" => new BsonDocument
            {
                { "ok", 1 },
                { "authInfo", new BsonDocument { { "authenticatedUsers", new BsonArray() }, { "authenticatedUserRoles", new BsonArray() } } }
            },
            "listcommands" => new BsonDocument
            {
                { "ok", 1 },
                { "commands", new BsonDocument
                    {
                        { "ping", new BsonDocument("help", "a]") },
                        { "buildInfo", new BsonDocument("help", "build info") },
                        { "count", new BsonDocument("help", "count documents") }
                    }
                }
            },
            "collstats" => HandleCollStats(firstElement.Value.AsString),
            "dbstats" => HandleDbStats(),
            "count" => HandleCount(cmd),
            "distinct" => HandleDistinct(cmd),
            "create" => HandleCreate(cmd),
            "drop" => HandleDrop(firstElement.Value.AsString),
            "createindexes" or "dropindexes" => new BsonDocument("ok", 1), // Index stubs
            "aggregate" => HandleAggregate(cmd),
            _ => new BsonDocument
            {
                { "ok", 0 },
                { "errmsg", $"no such command: '{commandName}'" },
                { "code", 59 }
            }
        };
    }

    private BsonDocument HandleCollStats(string collectionName)
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/collStats/
        var store = GetStore(collectionName);
        var count = store?.Count ?? 0;
        return new BsonDocument
        {
            { "ok", 1 },
            { "ns", $"{DatabaseNamespace.DatabaseName}.{collectionName}" },
            { "count", count },
            { "size", count * 100 }, // Approximate
            { "avgObjSize", count > 0 ? 100 : 0 },
            { "storageSize", 0 },
            { "nindexes", 1 },
            { "totalIndexSize", 0 }
        };
    }

    private BsonDocument HandleDbStats()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/dbStats/
        var totalDocs = _stores.Values.Sum(s => s.Count);
        return new BsonDocument
        {
            { "ok", 1 },
            { "db", DatabaseNamespace.DatabaseName },
            { "collections", _stores.Count },
            { "views", _views.Count },
            { "objects", totalDocs },
            { "avgObjSize", totalDocs > 0 ? 100.0 : 0.0 },
            { "dataSize", totalDocs * 100.0 },
            { "storageSize", 0.0 },
            { "indexes", _stores.Count },
            { "indexSize", 0.0 }
        };
    }

    private BsonDocument HandleCount(BsonDocument cmd)
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/count/
        var collName = cmd.GetElement(0).Value.AsString;
        var store = GetStore(collName);
        if (store == null)
            return new BsonDocument { { "ok", 1 }, { "n", 0 } };

        var docs = store.GetAll();
        if (cmd.Contains("query"))
        {
            var filter = cmd["query"].AsBsonDocument;
            docs = docs.Where(d => BsonFilterEvaluator.Matches(d, filter)).ToList();
        }

        return new BsonDocument { { "ok", 1 }, { "n", docs.Count } };
    }

    private BsonDocument HandleDistinct(BsonDocument cmd)
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/distinct/
        var collName = cmd.GetElement(0).Value.AsString;
        var key = cmd["key"].AsString;
        var store = GetStore(collName);
        if (store == null)
            return new BsonDocument { { "ok", 1 }, { "values", new BsonArray() } };

        var docs = (IReadOnlyList<BsonDocument>)store.GetAll();
        if (cmd.Contains("query"))
        {
            var filter = cmd["query"].AsBsonDocument;
            docs = docs.Where(d => BsonFilterEvaluator.Matches(d, filter)).ToList();
        }

        var values = docs.Select(d => BsonFilterEvaluator.ResolveFieldPath(d, key))
            .Where(v => v != BsonNull.Value)
            .Distinct(BsonValueComparer.Instance)
            .ToList();

        return new BsonDocument { { "ok", 1 }, { "values", new BsonArray(values) } };
    }

    private BsonDocument HandleCreate(BsonDocument cmd)
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/create/
        var collName = cmd.GetElement(0).Value.AsString;
        GetOrCreateStore(collName);
        _explicitlyCreated.TryAdd(collName, true);

        // Ref: https://www.mongodb.com/docs/manual/core/timeseries-collections/
        if (cmd.Contains("timeseries"))
            _timeSeriesOptions[collName] = cmd["timeseries"].AsBsonDocument;

        // Ref: https://www.mongodb.com/docs/manual/core/schema-validation/
        if (cmd.Contains("validator"))
        {
            _validators[collName] = new SchemaValidation(
                cmd["validator"].AsBsonDocument,
                cmd.Contains("validationAction") ? cmd["validationAction"].AsString : "error",
                cmd.Contains("validationLevel") ? cmd["validationLevel"].AsString : "strict");
        }

        return new BsonDocument("ok", 1);
    }

    private BsonDocument HandleDrop(string collectionName)
    {
        _stores.TryRemove(collectionName, out _);
        _explicitlyCreated.TryRemove(collectionName, out _);
        _views.TryRemove(collectionName, out _);
        _validators.TryRemove(collectionName, out _);
        _timeSeriesOptions.TryRemove(collectionName, out _);
        return new BsonDocument("ok", 1);
    }

    private BsonDocument HandleAggregate(BsonDocument cmd)
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/aggregate/
        var collName = cmd.GetElement(0).Value.AsString;
        var pipelineArray = cmd["pipeline"].AsBsonArray;
        var stages = pipelineArray.Select(s => s.AsBsonDocument).ToList();

        IEnumerable<BsonDocument> input;
        var store = GetStore(collName);
        input = store?.GetAll() ?? Enumerable.Empty<BsonDocument>();

        var client = Client as InMemoryMongoClient;
        var context = new AggregationContext(this, client) { CollectionNamespace = $"{DatabaseNamespace.DatabaseName}.{collName}" };
        var results = AggregationPipelineExecutor.Execute(input, stages, context).ToList();

        return new BsonDocument
        {
            { "ok", 1 },
            { "cursor", new BsonDocument
                {
                    { "firstBatch", new BsonArray(results) },
                    { "id", 0L },
                    { "ns", $"{DatabaseNamespace.DatabaseName}.{collName}" }
                }
            }
        };
    }

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

    #region CreateView

    // Ref: https://www.mongodb.com/docs/manual/core/views/
    //   "A MongoDB view is a read-only queryable object whose contents are defined by an aggregation
    //    pipeline on other collections or views."

    public void CreateView<TDocument, TResult>(string viewName, string viewOn, PipelineDefinition<TDocument, TResult> pipeline, CreateViewOptions<TDocument>? options = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var registry = BsonSerializer.SerializerRegistry;
        var serializer = registry.GetSerializer<TDocument>();
        var rendered = pipeline.Render(new RenderArgs<TDocument>(serializer, registry));
        var stages = new BsonArray(rendered.Documents);
        _views[viewName] = (viewOn, stages);
        // Ensure the source collection store exists
        GetOrCreateStore(viewOn);
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

    #region Watch

    public IChangeStreamCursor<TResult> Watch<TResult>(PipelineDefinition<ChangeStreamDocument<BsonDocument>, TResult> pipeline, ChangeStreamOptions? options = null, CancellationToken cancellationToken = default)
    {
        var notifier = (Client as InMemoryMongoClient)?.ChangeNotifier
            ?? throw new NotSupportedException("Watch requires InMemoryMongoClient.");
        var registry = BsonSerializer.SerializerRegistry;
        var outputSerializer = registry.GetSerializer<TResult>();
        return new InMemoryChangeStreamCursor<TResult>(
            notifier,
            databaseFilter: DatabaseNamespace.DatabaseName,
            collectionFilter: null, // database-level: all collections in this db
            options,
            outputSerializer,
            startSequence: notifier.CurrentSequence);
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
        foreach (var kvp in _stores) db._stores.TryAdd(kvp.Key, kvp.Value);
        foreach (var kvp in _explicitlyCreated) db._explicitlyCreated.TryAdd(kvp.Key, kvp.Value);
        foreach (var kvp in _views) db._views.TryAdd(kvp.Key, kvp.Value);
        foreach (var kvp in _validators) db._validators.TryAdd(kvp.Key, kvp.Value);
        foreach (var kvp in _timeSeriesOptions) db._timeSeriesOptions.TryAdd(kvp.Key, kvp.Value);
        return db;
    }

    public IMongoDatabase WithWriteConcern(WriteConcern writeConcern)
    {
        var settings = Settings.Clone();
        settings.WriteConcern = writeConcern;
        var db = new InMemoryMongoDatabase(DatabaseNamespace, Client, settings);
        foreach (var kvp in _stores) db._stores.TryAdd(kvp.Key, kvp.Value);
        foreach (var kvp in _explicitlyCreated) db._explicitlyCreated.TryAdd(kvp.Key, kvp.Value);
        foreach (var kvp in _views) db._views.TryAdd(kvp.Key, kvp.Value);
        foreach (var kvp in _validators) db._validators.TryAdd(kvp.Key, kvp.Value);
        foreach (var kvp in _timeSeriesOptions) db._timeSeriesOptions.TryAdd(kvp.Key, kvp.Value);
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
        foreach (var kvp in _views)
            db._views.TryAdd(kvp.Key, kvp.Value);
        foreach (var kvp in _validators)
            db._validators.TryAdd(kvp.Key, kvp.Value);
        foreach (var kvp in _timeSeriesOptions)
            db._timeSeriesOptions.TryAdd(kvp.Key, kvp.Value);
        return db;
    }

    #endregion

    #region Schema Validation

    /// <summary>
    /// Validates a document against the collection's schema validator, if any.
    /// Returns true if validation passes or no validator is set.
    /// </summary>
    internal bool ValidateDocument(string collectionName, BsonDocument doc, bool bypassDocumentValidation = false)
    {
        if (bypassDocumentValidation) return true;
        if (!_validators.TryGetValue(collectionName, out var validation)) return true;

        // Ref: https://www.mongodb.com/docs/manual/core/schema-validation/specify-json-schema/
        //   "Moderate validation: only validates existing valid docs on updates."
        // For simplicity, we always validate on strict level.

        var matches = BsonFilterEvaluator.Matches(doc, validation.Validator);
        if (!matches && validation.Action == "error")
        {
            // Ref: https://www.mongodb.com/docs/manual/reference/error-codes/
            //   Error code 121: DocumentValidationFailure
            var writeError = MongoErrors.CreateWriteError(ServerErrorCategory.Uncategorized, 121, "Document failed validation");
            throw new MongoWriteException(MongoErrors.SyntheticConnectionId, writeError, writeConcernError: null, innerException: null);
        }

        return matches;
    }

    internal SchemaValidation? GetValidator(string collectionName)
    {
        return _validators.TryGetValue(collectionName, out var v) ? v : null;
    }

    #endregion

    #region State Persistence

    /// <summary>
    /// Export the entire database state as a JSON string.
    /// </summary>
    // Ref: Inspired by CosmosDB.InMemoryEmulator state persistence pattern.
    public string ExportState()
    {
        var state = new BsonDocument();
        foreach (var kvp in _stores)
        {
            state[kvp.Key] = new BsonArray(kvp.Value.GetAll());
        }
        return state.ToJson();
    }

    /// <summary>
    /// Import database state from a JSON string, replacing existing data.
    /// </summary>
    public void ImportState(string json)
    {
        var state = BsonDocument.Parse(json);
        foreach (var element in state)
        {
            var store = GetOrCreateStore(element.Name);
            store.Clear();
            foreach (var doc in element.Value.AsBsonArray)
            {
                store.Insert(doc.AsBsonDocument.DeepClone().AsBsonDocument);
            }
        }
    }

    /// <summary>
    /// Export database state to a file.
    /// </summary>
    public void ExportStateToFile(string path)
    {
        File.WriteAllText(path, ExportState());
    }

    /// <summary>
    /// Import database state from a file.
    /// </summary>
    public void ImportStateFromFile(string path)
    {
        ImportState(File.ReadAllText(path));
    }

    /// <summary>
    /// Clears all documents from all collections.
    /// </summary>
    public void ClearDocuments()
    {
        foreach (var store in _stores.Values)
            store.Clear();
    }

    #endregion
}

/// <summary>
/// Schema validation configuration for a collection.
/// </summary>
internal record SchemaValidation(BsonDocument Validator, string Action, string Level);
