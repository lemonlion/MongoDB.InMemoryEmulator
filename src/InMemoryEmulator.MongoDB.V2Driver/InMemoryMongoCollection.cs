using global::MongoDB.Bson;
using global::MongoDB.Bson.Serialization;
using global::MongoDB.Driver;
using global::MongoDB.Driver.Linq;
using global::MongoDB.Driver.Search;

namespace InMemoryEmulator.MongoDB;

/// <summary>
/// In-memory implementation of <see cref="IMongoCollection{TDocument}"/> providing
/// zero-latency, in-process document storage for testing.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/drivers/csharp/current/
///   IMongoCollection is the primary interface for interacting with a MongoDB collection.
///   All SDK fluent APIs (Find, Aggregate, AsQueryable) route through this interface.
///
/// This is the MongoDB.Driver 2.x variant. Key differences from the 3.x version:
///   - Count / CountAsync are NOT marked [Obsolete] (they are deprecated in 3.x but valid in 2.x).
///   - ArrayFilterDefinition.Render requires a LinqProvider parameter in 2.30.0.
/// </remarks>
public class InMemoryMongoCollection<TDocument> : IMongoCollection<TDocument>
{
    private readonly DocumentStore _store;
    private readonly IBsonSerializer<TDocument> _serializer;
    private readonly InMemoryIndexManager<TDocument> _indexManager;
    private readonly List<BsonDocument>? _viewPipeline;
    private readonly CommandEventEmitter? _commandEventEmitter;

    /// <summary>
    /// Delegate for injecting faults into collection operations.
    /// Parameters: (operationType, filter/document as BsonDocument).
    /// Throw an exception to simulate a fault.
    /// </summary>
    public Action<string, BsonDocument?>? FaultInjector { get; set; }

    /// <summary>
    /// Operation log recording all operations for test assertions.
    /// </summary>
    public OperationLog OperationLog { get; } = new();

    internal InMemoryMongoCollection(
        CollectionNamespace collectionNamespace,
        IMongoDatabase database,
        DocumentStore store,
        MongoCollectionSettings? settings = null,
        List<BsonDocument>? viewPipeline = null,
        CommandEventEmitter? commandEventEmitter = null)
    {
        CollectionNamespace = collectionNamespace ?? throw new ArgumentNullException(nameof(collectionNamespace));
        Database = database ?? throw new ArgumentNullException(nameof(database));
        _store = store ?? throw new ArgumentNullException(nameof(store));
        Settings = settings ?? new MongoCollectionSettings();
        _serializer = BsonSerializer.LookupSerializer<TDocument>();
        _indexManager = new InMemoryIndexManager<TDocument>(this, _store);
        _viewPipeline = viewPipeline;
        _commandEventEmitter = commandEventEmitter;
    }

    /// <summary>
    /// Creates a standalone in-memory collection (no database/client) for simple unit tests.
    /// </summary>
    public InMemoryMongoCollection(string collectionName, string databaseName = "test")
    {
        var store = new DocumentStore();
        _store = store;
        Settings = new MongoCollectionSettings();
        _serializer = BsonSerializer.LookupSerializer<TDocument>();

        // Create a minimal database/client chain for the collection
        var client = new InMemoryMongoClient();
        var db = (InMemoryMongoDatabase)client.GetDatabase(databaseName);
        Database = db;
        CollectionNamespace = new CollectionNamespace(databaseName, collectionName);

        // Register this store in the database
        db.RegisterExternalStore(collectionName, _store);
        _indexManager = new InMemoryIndexManager<TDocument>(this, _store);
    }

    public CollectionNamespace CollectionNamespace { get; }
    public IMongoDatabase Database { get; }
    public IBsonSerializer<TDocument> DocumentSerializer => _serializer;
    public IMongoIndexManager<TDocument> Indexes => _indexManager;
    public IMongoSearchIndexManager SearchIndexes => throw new NotSupportedException("Search indexes are not supported in the in-memory emulator. Use a real MongoDB Atlas instance for $search/$searchMeta.");
    public MongoCollectionSettings Settings { get; }

    internal DocumentStore Store => _store;

    #region Insert

    // Ref: https://www.mongodb.com/docs/manual/reference/command/insert/
    //   "Inserts one or more documents and returns a document containing the status of all inserts."

    public void InsertOne(TDocument document, InsertOneOptions? options = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var bson = SerializeDocument(document);

        if (_commandEventEmitter != null)
        {
            _commandEventEmitter.EmitCommandEvents(
                "insert",
                CollectionNamespace.DatabaseNamespace,
                () => new BsonDocument { { "insert", CollectionNamespace.CollectionName }, { "documents", new BsonArray { bson.DeepClone() } } },
                () =>
                {
                    FaultInjector?.Invoke("insert", bson);
                    DocumentStore.EnsureId(bson);
                    _indexManager.ValidateDocument(bson);
                    ValidateSchemaOnWrite(bson);
                    _store.Insert(bson);
                    OperationLog.Record(new OperationRecord { Type = "InsertOne", Document = bson.DeepClone().AsBsonDocument });
                    WriteBackId(document, bson);
                    PublishChangeEvent(ChangeStreamOperationType.Insert, bson);
                },
                replyBuilder: () => new BsonDocument { { "ok", 1 }, { "n", 1 } });
        }
        else
        {
            FaultInjector?.Invoke("insert", bson);
            DocumentStore.EnsureId(bson);
            _indexManager.ValidateDocument(bson);
            ValidateSchemaOnWrite(bson);
            _store.Insert(bson);
            OperationLog.Record(new OperationRecord { Type = "InsertOne", Document = bson.DeepClone().AsBsonDocument });
            WriteBackId(document, bson);
            PublishChangeEvent(ChangeStreamOperationType.Insert, bson);
        }
    }

    public void InsertOne(IClientSessionHandle session, TDocument document, InsertOneOptions? options = null, CancellationToken cancellationToken = default)
    {
        InsertOne(document, options, cancellationToken);
    }

    public Task InsertOneAsync(TDocument document, CancellationToken cancellationToken)
    {
        InsertOne(document, options: null, cancellationToken);
        return Task.CompletedTask;
    }

    public Task InsertOneAsync(TDocument document, InsertOneOptions? options = null, CancellationToken cancellationToken = default)
    {
        InsertOne(document, options, cancellationToken);
        return Task.CompletedTask;
    }

    public Task InsertOneAsync(IClientSessionHandle session, TDocument document, InsertOneOptions? options = null, CancellationToken cancellationToken = default)
    {
        InsertOne(document, options, cancellationToken);
        return Task.CompletedTask;
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/command/insert/
    //   "If ordered is true (default), the server inserts documents serially and stops on first error.
    //    If ordered is false, the server inserts all documents and returns errors for any failures."
    public void InsertMany(IEnumerable<TDocument> documents, InsertManyOptions? options = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var docList = documents?.ToList() ?? throw new ArgumentNullException(nameof(documents));
        var isOrdered = options?.IsOrdered ?? true;
        var errors = new List<BulkWriteError>();

        for (int i = 0; i < docList.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                var bson = SerializeDocument(docList[i]);
                DocumentStore.EnsureId(bson);
                _indexManager.ValidateDocument(bson);
                ValidateSchemaOnWrite(bson);
                _store.Insert(bson);
                WriteBackId(docList[i], bson);
                // Ref: https://www.mongodb.com/docs/manual/changeStreams/
                //   Change streams report insertions whether from InsertOne or InsertMany.
                PublishChangeEvent(ChangeStreamOperationType.Insert, bson);
            }
            catch (MongoWriteException ex) when (!isOrdered)
            {
                // Ref: https://www.mongodb.com/docs/manual/reference/method/db.collection.insertMany/
                //   "Unordered inserts collect all errors and report them together."
                //   Preserve the actual error code and category from the original exception.
                var errorCode = ex.WriteError?.Code ?? 11000;
                var errorCategory = ex.WriteError?.Category ?? ServerErrorCategory.DuplicateKey;
                errors.Add(MongoErrors.CreateBulkWriteError(i, errorCategory, errorCode, ex.Message));
            }
            catch (MongoWriteException ex) when (isOrdered)
            {
                // Ref: https://www.mongodb.com/docs/manual/reference/method/db.collection.insertMany/
                //   "InsertMany errors are always wrapped in MongoBulkWriteException."
                var errorCode = ex.WriteError?.Code ?? 11000;
                var errorCategory = ex.WriteError?.Category ?? ServerErrorCategory.DuplicateKey;
                var writeErrors = new List<BulkWriteError>
                {
                    MongoErrors.CreateBulkWriteError(i, errorCategory, errorCode, ex.Message)
                };
                throw new MongoBulkWriteException<TDocument>(
                    MongoErrors.SyntheticConnectionId,
                    result: new BulkWriteResult<TDocument>.Acknowledged(
                        requestCount: docList.Count,
                        matchedCount: 0,
                        deletedCount: 0,
                        insertedCount: i,
                        modifiedCount: 0,
                        processedRequests: docList.Take(i + 1).Select(d => (WriteModel<TDocument>)new InsertOneModel<TDocument>(d)).ToList(),
                        upserts: new List<BulkWriteUpsert>()),
                    writeErrors: writeErrors,
                    writeConcernError: null,
                    unprocessedRequests: docList.Skip(i + 1).Select(d => (WriteModel<TDocument>)new InsertOneModel<TDocument>(d)).ToList());
            }
        }

        if (errors.Count > 0)
        {
            throw new MongoBulkWriteException<TDocument>(
                MongoErrors.SyntheticConnectionId,
                result: new BulkWriteResult<TDocument>.Acknowledged(
                    requestCount: docList.Count,
                    matchedCount: 0,
                    deletedCount: 0,
                    insertedCount: docList.Count - errors.Count,
                    modifiedCount: 0,
                    processedRequests: docList.Select((d, i) => (WriteModel<TDocument>)new InsertOneModel<TDocument>(d)).ToList(),
                    upserts: new List<BulkWriteUpsert>()),
                writeErrors: errors,
                writeConcernError: null,
                unprocessedRequests: new List<WriteModel<TDocument>>());
        }
    }

    public void InsertMany(IClientSessionHandle session, IEnumerable<TDocument> documents, InsertManyOptions? options = null, CancellationToken cancellationToken = default)
    {
        InsertMany(documents, options, cancellationToken);
    }

    public Task InsertManyAsync(IEnumerable<TDocument> documents, InsertManyOptions? options = null, CancellationToken cancellationToken = default)
    {
        InsertMany(documents, options, cancellationToken);
        return Task.CompletedTask;
    }

    public Task InsertManyAsync(IClientSessionHandle session, IEnumerable<TDocument> documents, InsertManyOptions? options = null, CancellationToken cancellationToken = default)
    {
        InsertMany(documents, options, cancellationToken);
        return Task.CompletedTask;
    }

    #endregion

    #region Find

    // Ref: https://www.mongodb.com/docs/manual/reference/command/find/
    //   "The find command returns a cursor to the documents that match the query criteria."

    public IAsyncCursor<TDocument> FindSync(FilterDefinition<TDocument> filter, FindOptions<TDocument, TDocument>? options = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var renderedFilterForLog = RenderFilter(filter);

        if (_commandEventEmitter != null)
        {
            List<BsonDocument>? capturedDocs = null;
            return _commandEventEmitter.EmitCommandEvents(
                "find",
                CollectionNamespace.DatabaseNamespace,
                () => new BsonDocument { { "find", CollectionNamespace.CollectionName }, { "filter", renderedFilterForLog?.DeepClone() ?? new BsonDocument() } },
                () =>
                {
                    FaultInjector?.Invoke("find", renderedFilterForLog);
                    OperationLog.Record(new OperationRecord { Type = "Find", Filter = renderedFilterForLog?.DeepClone().AsBsonDocument });
                    if (options?.CursorType is (CursorType.Tailable or CursorType.TailableAwait) && _store.IsCapped)
                    {
                        var renderedFilter = RenderFilter(filter) ?? new BsonDocument();
                        return (IAsyncCursor<TDocument>)new InMemoryTailableCursor<TDocument>(
                            _store, renderedFilter, _serializer,
                            awaitData: options.CursorType == CursorType.TailableAwait,
                            maxAwaitTime: options.MaxAwaitTime);
                    }
                    var docs = FindInternal(filter, options);
                    capturedDocs = docs.Select(d => SerializeDocument(d).DeepClone().AsBsonDocument).ToList();
                    var batchSize = options?.BatchSize ?? 101;
                    return new InMemoryAsyncCursor<TDocument>(docs, batchSize);
                },
                replyBuilder: _ => ReplyHelper.CursorReply(capturedDocs ?? [], CollectionNamespace));
        }

        FaultInjector?.Invoke("find", renderedFilterForLog);
        OperationLog.Record(new OperationRecord { Type = "Find", Filter = renderedFilterForLog?.DeepClone().AsBsonDocument });

        // Ref: https://www.mongodb.com/docs/manual/core/tailable-cursors/
        //   "Tailable cursors are only for use with capped collections."
        if (options?.CursorType is (CursorType.Tailable or CursorType.TailableAwait) && _store.IsCapped)
        {
            var renderedFilter = RenderFilter(filter) ?? new BsonDocument();
            return new InMemoryTailableCursor<TDocument>(
                _store,
                renderedFilter,
                _serializer,
                awaitData: options.CursorType == CursorType.TailableAwait,
                maxAwaitTime: options.MaxAwaitTime);
        }

        var docs = FindInternal(filter, options);
        var batchSize = options?.BatchSize ?? 101;
        return new InMemoryAsyncCursor<TDocument>(docs, batchSize);
    }

    public IAsyncCursor<TProjection> FindSync<TProjection>(FilterDefinition<TDocument> filter, FindOptions<TDocument, TProjection>? options = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var renderedFilterCheck = RenderFilter(filter);

        if (_commandEventEmitter != null)
        {
            List<BsonDocument>? capturedDocs = null;
            return _commandEventEmitter.EmitCommandEvents(
                "find",
                CollectionNamespace.DatabaseNamespace,
                () => new BsonDocument { { "find", CollectionNamespace.CollectionName }, { "filter", renderedFilterCheck?.DeepClone() ?? new BsonDocument() } },
                () =>
                {
                    FaultInjector?.Invoke("find", renderedFilterCheck);
                    OperationLog.Record(new OperationRecord { Type = "Find", Filter = renderedFilterCheck?.DeepClone().AsBsonDocument });
                    capturedDocs = FindInternalBson(filter).Select(d => d.DeepClone().AsBsonDocument).ToList();
                    return FindSyncProjectionCore(filter, options);
                },
                replyBuilder: _ => ReplyHelper.CursorReply(capturedDocs ?? [], CollectionNamespace));
        }

        FaultInjector?.Invoke("find", renderedFilterCheck);
        OperationLog.Record(new OperationRecord { Type = "Find", Filter = renderedFilterCheck?.DeepClone().AsBsonDocument });
        return FindSyncProjectionCore(filter, options);
    }

    private IAsyncCursor<TProjection> FindSyncProjectionCore<TProjection>(FilterDefinition<TDocument> filter, FindOptions<TDocument, TProjection>? options)
    {
        // Tailable cursor support for capped collections (when TProjection == TDocument)
        if (options?.CursorType is (CursorType.Tailable or CursorType.TailableAwait) && _store.IsCapped
            && typeof(TProjection) == typeof(TDocument))
        {
            var renderedFilter = RenderFilter(filter) ?? new BsonDocument();
            var tailable = new InMemoryTailableCursor<TDocument>(
                _store,
                renderedFilter,
                _serializer,
                awaitData: options.CursorType == CursorType.TailableAwait,
                maxAwaitTime: options.MaxAwaitTime);
            return (IAsyncCursor<TProjection>)(object)tailable;
        }

        // Extract sort/skip/limit from options via reflection on base FindOptionsBase
        SortDefinition<TDocument>? sort = null;
        int? skip = null;
        int? limit = null;
        int batchSize = 101;
        BsonDocument? projectionDoc = null;

        if (options != null)
        {
            batchSize = options.BatchSize ?? 101;

            // FindOptions<TDoc, TProj> has Sort, Skip, Limit, Projection via FindOptionsBase
            // We need to render the sort/projection
            var registry = BsonSerializer.SerializerRegistry;

            if (options.Sort != null)
                sort = options.Sort;
            skip = options.Skip;
            limit = options.Limit;

            if (options.Projection != null)
            {
                var renderedProjection = options.Projection.Render(new RenderArgs<TDocument>(_serializer, registry));
                projectionDoc = renderedProjection.Document;
            }
        }

        var bsonResults = FindInternalBsonFull(filter, sort, skip, limit);

        // Apply projection
        if (projectionDoc != null && projectionDoc.ElementCount > 0)
            bsonResults = bsonResults.Select(doc => BsonProjectionEvaluator.Apply(doc, projectionDoc)).ToList();

        // Deserialize to target type
        if (typeof(TProjection) == typeof(TDocument))
        {
            var typed = bsonResults.Select(DeserializeDocument).ToList();
            return new InMemoryAsyncCursor<TProjection>((IEnumerable<TProjection>)(object)typed, batchSize);
        }

        var projected = bsonResults.Select(b => BsonSerializer.Deserialize<TProjection>(b)).ToList();
        return new InMemoryAsyncCursor<TProjection>(projected, batchSize);
    }

    public Task<IAsyncCursor<TDocument>> FindAsync(FilterDefinition<TDocument> filter, FindOptions<TDocument, TDocument>? options = null, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(FindSync(filter, options, cancellationToken));
    }

    public Task<IAsyncCursor<TProjection>> FindAsync<TProjection>(FilterDefinition<TDocument> filter, FindOptions<TDocument, TProjection>? options = null, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(FindSync(filter, options, cancellationToken));
    }

    public IAsyncCursor<TDocument> FindSync(IClientSessionHandle session, FilterDefinition<TDocument> filter, FindOptions<TDocument, TDocument>? options = null, CancellationToken cancellationToken = default)
        => FindSync(filter, options, cancellationToken);

    public IAsyncCursor<TProjection> FindSync<TProjection>(IClientSessionHandle session, FilterDefinition<TDocument> filter, FindOptions<TDocument, TProjection>? options = null, CancellationToken cancellationToken = default)
        => FindSync(filter, options, cancellationToken);

    public Task<IAsyncCursor<TDocument>> FindAsync(IClientSessionHandle session, FilterDefinition<TDocument> filter, FindOptions<TDocument, TDocument>? options = null, CancellationToken cancellationToken = default)
        => FindAsync(filter, options, cancellationToken);

    public Task<IAsyncCursor<TProjection>> FindAsync<TProjection>(IClientSessionHandle session, FilterDefinition<TDocument> filter, FindOptions<TDocument, TProjection>? options = null, CancellationToken cancellationToken = default)
        => FindAsync(filter, options, cancellationToken);

    #endregion

    #region Count

    // Ref: https://www.mongodb.com/docs/manual/reference/command/count/
    //   "Counts the number of documents in a collection or a view."

    public long CountDocuments(FilterDefinition<TDocument> filter, CountOptions? options = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        long count = FindInternal(filter, null).Count;

        // Ref: https://www.mongodb.com/docs/manual/reference/command/count/
        //   "skip: Optional. The number of matching documents to skip before returning results."
        //   "limit: Optional. The maximum number of matching documents to return."
        if (options?.Skip > 0)
            count = Math.Max(0, count - options.Skip.Value);
        if (options?.Limit > 0)
            count = Math.Min(count, options.Limit.Value);

        return count;
    }

    public long CountDocuments(IClientSessionHandle session, FilterDefinition<TDocument> filter, CountOptions? options = null, CancellationToken cancellationToken = default)
        => CountDocuments(filter, options, cancellationToken);

    public Task<long> CountDocumentsAsync(FilterDefinition<TDocument> filter, CountOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(CountDocuments(filter, options, cancellationToken));

    public Task<long> CountDocumentsAsync(IClientSessionHandle session, FilterDefinition<TDocument> filter, CountOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(CountDocuments(filter, options, cancellationToken));

    // In 2.x, Count/CountAsync are NOT obsolete (they are deprecated in 3.x but valid in 2.x)
    public long Count(FilterDefinition<TDocument> filter, CountOptions? options = null, CancellationToken cancellationToken = default)
        => CountDocuments(filter, options, cancellationToken);

    public long Count(IClientSessionHandle session, FilterDefinition<TDocument> filter, CountOptions? options = null, CancellationToken cancellationToken = default)
        => CountDocuments(filter, options, cancellationToken);

    public Task<long> CountAsync(FilterDefinition<TDocument> filter, CountOptions? options = null, CancellationToken cancellationToken = default)
        => CountDocumentsAsync(filter, options, cancellationToken);

    public Task<long> CountAsync(IClientSessionHandle session, FilterDefinition<TDocument> filter, CountOptions? options = null, CancellationToken cancellationToken = default)
        => CountDocumentsAsync(filter, options, cancellationToken);

    public long EstimatedDocumentCount(EstimatedDocumentCountOptions? options = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return _store.Count;
    }

    public Task<long> EstimatedDocumentCountAsync(EstimatedDocumentCountOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(EstimatedDocumentCount(options, cancellationToken));

    #endregion

    #region Delete

    // Ref: https://www.mongodb.com/docs/manual/reference/command/delete/
    //   "The delete command removes documents from a collection."

    public DeleteResult DeleteOne(FilterDefinition<TDocument> filter, CancellationToken cancellationToken = default)
        => DeleteOne(filter, options: null, cancellationToken);

    public DeleteResult DeleteOne(FilterDefinition<TDocument> filter, DeleteOptions? options, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var renderedFilter = RenderFilter(filter);

        if (_commandEventEmitter != null)
        {
            return _commandEventEmitter.EmitCommandEvents(
                "delete",
                CollectionNamespace.DatabaseNamespace,
                () => new BsonDocument { { "delete", CollectionNamespace.CollectionName }, { "deletes", new BsonArray { new BsonDocument { { "q", renderedFilter?.DeepClone() ?? new BsonDocument() }, { "limit", 1 } } } } },
                () => DeleteOneCore(filter, renderedFilter),
                replyBuilder: result => ReplyHelper.DeleteReply(result));
        }

        return DeleteOneCore(filter, renderedFilter);
    }

    private DeleteResult DeleteOneCore(FilterDefinition<TDocument> filter, BsonDocument? renderedFilter)
    {
        FaultInjector?.Invoke("delete", renderedFilter);
        OperationLog.Record(new OperationRecord { Type = "DeleteOne", Filter = renderedFilter?.DeepClone().AsBsonDocument });
        var matches = FindInternalBson(filter);
        if (matches.Count == 0)
            return new DeleteResult.Acknowledged(0);

        var toDelete = matches[0];
        _store.Remove(toDelete["_id"]);
        PublishChangeEvent(ChangeStreamOperationType.Delete, toDelete);
        return new DeleteResult.Acknowledged(1);
    }

    public DeleteResult DeleteOne(IClientSessionHandle session, FilterDefinition<TDocument> filter, DeleteOptions? options = null, CancellationToken cancellationToken = default)
        => DeleteOne(filter, options, cancellationToken);

    public Task<DeleteResult> DeleteOneAsync(FilterDefinition<TDocument> filter, CancellationToken cancellationToken = default)
        => Task.FromResult(DeleteOne(filter, cancellationToken: cancellationToken));

    public Task<DeleteResult> DeleteOneAsync(FilterDefinition<TDocument> filter, DeleteOptions? options, CancellationToken cancellationToken = default)
        => Task.FromResult(DeleteOne(filter, options, cancellationToken));

    public Task<DeleteResult> DeleteOneAsync(IClientSessionHandle session, FilterDefinition<TDocument> filter, DeleteOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(DeleteOne(filter, options, cancellationToken));

    public DeleteResult DeleteMany(FilterDefinition<TDocument> filter, CancellationToken cancellationToken = default)
        => DeleteMany(filter, options: null, cancellationToken);

    public DeleteResult DeleteMany(FilterDefinition<TDocument> filter, DeleteOptions? options, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var renderedFilter = RenderFilter(filter);

        if (_commandEventEmitter != null)
        {
            return _commandEventEmitter.EmitCommandEvents(
                "delete",
                CollectionNamespace.DatabaseNamespace,
                () => new BsonDocument { { "delete", CollectionNamespace.CollectionName }, { "deletes", new BsonArray { new BsonDocument { { "q", renderedFilter?.DeepClone() ?? new BsonDocument() }, { "limit", 0 } } } } },
                () => DeleteManyCore(filter, renderedFilter),
                replyBuilder: result => ReplyHelper.DeleteReply(result));
        }

        return DeleteManyCore(filter, renderedFilter);
    }

    private DeleteResult DeleteManyCore(FilterDefinition<TDocument> filter, BsonDocument? renderedFilter)
    {
        FaultInjector?.Invoke("delete", renderedFilter);
        OperationLog.Record(new OperationRecord { Type = "DeleteMany", Filter = renderedFilter?.DeepClone().AsBsonDocument });
        var matches = FindInternalBson(filter);
        foreach (var doc in matches)
        {
            _store.Remove(doc["_id"]);
            PublishChangeEvent(ChangeStreamOperationType.Delete, doc);
        }
        return new DeleteResult.Acknowledged(matches.Count);
    }

    public DeleteResult DeleteMany(IClientSessionHandle session, FilterDefinition<TDocument> filter, DeleteOptions? options = null, CancellationToken cancellationToken = default)
        => DeleteMany(filter, options, cancellationToken);

    public Task<DeleteResult> DeleteManyAsync(FilterDefinition<TDocument> filter, CancellationToken cancellationToken = default)
        => Task.FromResult(DeleteMany(filter, cancellationToken: cancellationToken));

    public Task<DeleteResult> DeleteManyAsync(FilterDefinition<TDocument> filter, DeleteOptions? options, CancellationToken cancellationToken = default)
        => Task.FromResult(DeleteMany(filter, options, cancellationToken));

    public Task<DeleteResult> DeleteManyAsync(IClientSessionHandle session, FilterDefinition<TDocument> filter, DeleteOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(DeleteMany(filter, options, cancellationToken));

    #endregion

    #region Replace

    // Ref: https://www.mongodb.com/docs/manual/reference/command/update/
    //   "Replaces a single document within the collection based on the filter."

    public ReplaceOneResult ReplaceOne(FilterDefinition<TDocument> filter, TDocument replacement, ReplaceOptions? options = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var renderedFilter = RenderFilter(filter);

        if (_commandEventEmitter != null)
        {
            return _commandEventEmitter.EmitCommandEvents(
                "update",
                CollectionNamespace.DatabaseNamespace,
                () => new BsonDocument { { "update", CollectionNamespace.CollectionName }, { "updates", new BsonArray { new BsonDocument { { "q", renderedFilter?.DeepClone() ?? new BsonDocument() }, { "u", SerializeDocument(replacement).DeepClone() } } } } },
                () => ReplaceOneCore(filter, replacement, options, renderedFilter),
                replyBuilder: result => ReplyHelper.ReplaceReply(result));
        }

        return ReplaceOneCore(filter, replacement, options, renderedFilter);
    }

    private ReplaceOneResult ReplaceOneCore(FilterDefinition<TDocument> filter, TDocument replacement, ReplaceOptions? options, BsonDocument? renderedFilter)
    {
        FaultInjector?.Invoke("update", renderedFilter);
        OperationLog.Record(new OperationRecord { Type = "ReplaceOne", Filter = renderedFilter?.DeepClone().AsBsonDocument });
        var replacementBson = SerializeDocument(replacement);

        // Validate replacement doc doesn't contain update operators
        if (replacementBson.Names.Any(n => n.StartsWith("$")))
            throw MongoErrors.BadValue("the replace operation document must not contain atomic operators");

        var matches = FindInternalBson(filter);

        if (matches.Count == 0)
        {
            if (options?.IsUpsert == true)
            {
                // Ref: https://www.mongodb.com/docs/manual/reference/method/db.collection.replaceOne/
                //   "If upsert: true and no documents match the filter, the new document
                //    uses the _id from the replacement or the equality _id from the filter."
                if (!replacementBson.Contains("_id") || replacementBson["_id"] == BsonNull.Value)
                {
                    var filterDoc = CreateUpsertDocumentFromFilter(filter);
                    if (filterDoc.Contains("_id") && filterDoc["_id"] != BsonNull.Value)
                    {
                        replacementBson.Remove("_id");
                        replacementBson.InsertAt(0, new BsonElement("_id", filterDoc["_id"]));
                    }
                }
                DocumentStore.EnsureId(replacementBson);
                _indexManager.ValidateDocument(replacementBson);
                ValidateSchemaOnWrite(replacementBson);
                var doc = _store.Insert(replacementBson);
                PublishChangeEvent(ChangeStreamOperationType.Insert, doc);
                var upsertedId = doc["_id"];
                return new ReplaceOneResult.Acknowledged(0, 0, upsertedId);
            }
            return new ReplaceOneResult.Acknowledged(0, 0, null);
        }

        var target = matches[0];
        var targetId = target["_id"];

        // Ref: https://www.mongodb.com/docs/manual/reference/error-codes/
        //   Error code 66: if replacement _id differs from matched doc _id
        if (replacementBson.Contains("_id") && replacementBson["_id"] != BsonNull.Value
            && !replacementBson["_id"].Equals(targetId))
            throw MongoErrors.ImmutableField("_id");

        replacementBson["_id"] = targetId;
        _indexManager.ValidateDocument(replacementBson, excludeId: targetId);
        ValidateSchemaOnWrite(replacementBson);
        var beforeChange = target.DeepClone().AsBsonDocument;
        var (_, modified) = _store.Replace(targetId, replacementBson);

        // Ref: https://www.mongodb.com/docs/manual/changeStreams/
        //   Change stream events are only emitted when the document is actually modified.
        if (modified)
            PublishChangeEvent(ChangeStreamOperationType.Replace, replacementBson, beforeChange);
        return new ReplaceOneResult.Acknowledged(1, modified ? 1 : 0, null);
    }

    [Obsolete("Use the overload with ReplaceOptions instead.")]
    public ReplaceOneResult ReplaceOne(FilterDefinition<TDocument> filter, TDocument replacement, UpdateOptions? options, CancellationToken cancellationToken = default)
        => ReplaceOne(filter, replacement, new ReplaceOptions { IsUpsert = options?.IsUpsert ?? false }, cancellationToken);

    public ReplaceOneResult ReplaceOne(IClientSessionHandle session, FilterDefinition<TDocument> filter, TDocument replacement, ReplaceOptions? options = null, CancellationToken cancellationToken = default)
        => ReplaceOne(filter, replacement, options, cancellationToken);

    [Obsolete("Use the overload with ReplaceOptions instead.")]
    public ReplaceOneResult ReplaceOne(IClientSessionHandle session, FilterDefinition<TDocument> filter, TDocument replacement, UpdateOptions? options, CancellationToken cancellationToken = default)
        => ReplaceOne(filter, replacement, new ReplaceOptions { IsUpsert = options?.IsUpsert ?? false }, cancellationToken);

    public Task<ReplaceOneResult> ReplaceOneAsync(FilterDefinition<TDocument> filter, TDocument replacement, ReplaceOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(ReplaceOne(filter, replacement, options, cancellationToken));

    [Obsolete("Use the overload with ReplaceOptions instead.")]
    public Task<ReplaceOneResult> ReplaceOneAsync(FilterDefinition<TDocument> filter, TDocument replacement, UpdateOptions? options, CancellationToken cancellationToken = default)
        => Task.FromResult(ReplaceOne(filter, replacement, new ReplaceOptions { IsUpsert = options?.IsUpsert ?? false }, cancellationToken));

    public Task<ReplaceOneResult> ReplaceOneAsync(IClientSessionHandle session, FilterDefinition<TDocument> filter, TDocument replacement, ReplaceOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(ReplaceOne(filter, replacement, options, cancellationToken));

    [Obsolete("Use the overload with ReplaceOptions instead.")]
    public Task<ReplaceOneResult> ReplaceOneAsync(IClientSessionHandle session, FilterDefinition<TDocument> filter, TDocument replacement, UpdateOptions? options, CancellationToken cancellationToken = default)
        => Task.FromResult(ReplaceOne(filter, replacement, new ReplaceOptions { IsUpsert = options?.IsUpsert ?? false }, cancellationToken));

    #endregion

    #region Update

    // Ref: https://www.mongodb.com/docs/manual/reference/command/update/
    //   "The update command modifies documents in a collection."

    public UpdateResult UpdateOne(FilterDefinition<TDocument> filter, UpdateDefinition<TDocument> update, UpdateOptions? options = null, CancellationToken cancellationToken = default)
    {
        // Ref: https://www.mongodb.com/docs/drivers/csharp/current/fundamentals/crud/write-operations/
        //   "If the write operation fails, the driver throws a MongoWriteException."
        if (_commandEventEmitter != null)
        {
            var renderedFilter = RenderFilter(filter);
            return _commandEventEmitter.EmitCommandEvents(
                "update",
                CollectionNamespace.DatabaseNamespace,
                () => new BsonDocument { { "update", CollectionNamespace.CollectionName }, { "updates", new BsonArray { new BsonDocument { { "q", renderedFilter?.DeepClone() ?? new BsonDocument() }, { "multi", false } } } } },
                () =>
                {
                    try { return UpdateOneCore(filter, update, options, cancellationToken); }
                    catch (MongoCommandException ex) { throw MongoErrors.WrapAsWriteError(ex); }
                },
                replyBuilder: result => ReplyHelper.UpdateReply(result));
        }

        try
        {
            return UpdateOneCore(filter, update, options, cancellationToken);
        }
        catch (MongoCommandException ex)
        {
            throw MongoErrors.WrapAsWriteError(ex);
        }
    }

    private UpdateResult UpdateOneCore(FilterDefinition<TDocument> filter, UpdateDefinition<TDocument> update, UpdateOptions? options, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var updateBson = RenderUpdate(update);
        var renderedFilter = RenderFilter(filter);
        FaultInjector?.Invoke("update", renderedFilter);
        OperationLog.Record(new OperationRecord { Type = "UpdateOne", Filter = renderedFilter?.DeepClone().AsBsonDocument, Update = updateBson is BsonDocument ud ? ud.DeepClone().AsBsonDocument : null });
        ValidateUpdate(updateBson);

        // Ref: https://www.mongodb.com/docs/manual/reference/method/db.collection.updateOne/
        //   "arrayFilters: An array of filter documents that determine which array elements to modify."
        var renderedArrayFilters = RenderArrayFilters(options?.ArrayFilters);

        var matches = FindInternalBson(filter);

        if (matches.Count == 0)
        {
            if (options?.IsUpsert == true)
            {
                var upsertDoc = updateBson is BsonArray
                    ? CreateUpsertDocumentFromFilter(filter)
                    : CreateUpsertDocument(filter, updateBson.AsBsonDocument);
                var upsertResult = ApplyUpdate(upsertDoc, updateBson, isUpsertInsert: true,
                    arrayFilters: renderedArrayFilters);
                DocumentStore.EnsureId(upsertResult);
                _indexManager.ValidateDocument(upsertResult);
                ValidateSchemaOnWrite(upsertResult);
                var inserted = _store.Insert(upsertResult);
                PublishChangeEvent(ChangeStreamOperationType.Insert, inserted);
                return new UpdateResult.Acknowledged(0, 0, inserted["_id"]);
            }
            return new UpdateResult.Acknowledged(0, 0, null);
        }

        var target = matches[0];
        var targetId = target["_id"];

        // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/positional/
        //   "The positional $ operator acts as a placeholder for the first element
        //    that matches the query document."
        var (matchedIdx, matchedField) = renderedFilter != null
            ? BsonUpdateEvaluator.FindMatchedArrayIndex(target, renderedFilter)
            : (-1, (string?)null);

        // Pre-validate: compute the updated doc and check unique indexes before committing
        var previewDoc = ApplyUpdate(target.DeepClone().AsBsonDocument, updateBson, isUpsertInsert: false,
            arrayFilters: renderedArrayFilters, matchedArrayIndex: matchedIdx, matchedArrayField: matchedField);
        previewDoc["_id"] = targetId;
        _indexManager.ValidateDocument(previewDoc, excludeId: targetId);
        ValidateSchemaOnWrite(previewDoc);

        // Use DocumentStore.Update for atomic apply — prevents lost updates under concurrency
        var (matched, modified, beforeChange) = _store.Update(targetId, currentDoc =>
        {
            // Recompute matched index against the current doc state
            var (curIdx, curField) = renderedFilter != null
                ? BsonUpdateEvaluator.FindMatchedArrayIndex(currentDoc, renderedFilter)
                : (-1, (string?)null);
            var applied = ApplyUpdate(currentDoc, updateBson, isUpsertInsert: false,
                arrayFilters: renderedArrayFilters, matchedArrayIndex: curIdx, matchedArrayField: curField);
            applied["_id"] = targetId;
            return applied;
        });

        if (matched && modified && beforeChange != null)
        {
            var afterDoc = _store.Get(targetId);
            if (afterDoc != null) PublishChangeEvent(ChangeStreamOperationType.Update, afterDoc, beforeChange);
        }
        return new UpdateResult.Acknowledged(matched ? 1 : 0, modified ? 1 : 0, null);
    }

    public UpdateResult UpdateOne(IClientSessionHandle session, FilterDefinition<TDocument> filter, UpdateDefinition<TDocument> update, UpdateOptions? options = null, CancellationToken cancellationToken = default)
        => UpdateOne(filter, update, options, cancellationToken);

    public Task<UpdateResult> UpdateOneAsync(FilterDefinition<TDocument> filter, UpdateDefinition<TDocument> update, UpdateOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(UpdateOne(filter, update, options, cancellationToken));

    public Task<UpdateResult> UpdateOneAsync(IClientSessionHandle session, FilterDefinition<TDocument> filter, UpdateDefinition<TDocument> update, UpdateOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(UpdateOne(filter, update, options, cancellationToken));

    public UpdateResult UpdateMany(FilterDefinition<TDocument> filter, UpdateDefinition<TDocument> update, UpdateOptions? options = null, CancellationToken cancellationToken = default)
    {
        // Ref: https://www.mongodb.com/docs/drivers/csharp/current/fundamentals/crud/write-operations/
        //   "If the write operation fails, the driver throws a MongoWriteException."
        if (_commandEventEmitter != null)
        {
            var renderedFilter = RenderFilter(filter);
            return _commandEventEmitter.EmitCommandEvents(
                "update",
                CollectionNamespace.DatabaseNamespace,
                () => new BsonDocument { { "update", CollectionNamespace.CollectionName }, { "updates", new BsonArray { new BsonDocument { { "q", renderedFilter?.DeepClone() ?? new BsonDocument() }, { "multi", true } } } } },
                () =>
                {
                    try { return UpdateManyCore(filter, update, options, cancellationToken); }
                    catch (MongoCommandException ex) { throw MongoErrors.WrapAsWriteError(ex); }
                },
                replyBuilder: result => ReplyHelper.UpdateReply(result));
        }

        try
        {
            return UpdateManyCore(filter, update, options, cancellationToken);
        }
        catch (MongoCommandException ex)
        {
            throw MongoErrors.WrapAsWriteError(ex);
        }
    }

    private UpdateResult UpdateManyCore(FilterDefinition<TDocument> filter, UpdateDefinition<TDocument> update, UpdateOptions? options, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var updateBson = RenderUpdate(update);
        var renderedFilter = RenderFilter(filter);
        FaultInjector?.Invoke("update", renderedFilter);
        OperationLog.Record(new OperationRecord { Type = "UpdateMany", Filter = renderedFilter?.DeepClone().AsBsonDocument, Update = updateBson is BsonDocument ud ? ud.DeepClone().AsBsonDocument : null });
        ValidateUpdate(updateBson);

        var renderedArrayFilters = RenderArrayFilters(options?.ArrayFilters);

        var matches = FindInternalBson(filter);

        if (matches.Count == 0)
        {
            if (options?.IsUpsert == true)
            {
                var upsertDoc = updateBson is BsonArray
                    ? CreateUpsertDocumentFromFilter(filter)
                    : CreateUpsertDocument(filter, updateBson.AsBsonDocument);
                var upsertResult = ApplyUpdate(upsertDoc, updateBson, isUpsertInsert: true,
                    arrayFilters: renderedArrayFilters);
                DocumentStore.EnsureId(upsertResult);
                _indexManager.ValidateDocument(upsertResult);
                ValidateSchemaOnWrite(upsertResult);
                var inserted = _store.Insert(upsertResult);
                PublishChangeEvent(ChangeStreamOperationType.Insert, inserted);
                return new UpdateResult.Acknowledged(0, 0, inserted["_id"]);
            }
            return new UpdateResult.Acknowledged(0, 0, null);
        }

        long modifiedCount = 0;
        foreach (var target in matches)
        {
            var targetId = target["_id"];

            var (matchedIdx, matchedField) = renderedFilter != null
                ? BsonUpdateEvaluator.FindMatchedArrayIndex(target, renderedFilter)
                : (-1, (string?)null);

            // Pre-validate: compute the updated doc and check unique indexes before committing
            var previewDoc = ApplyUpdate(target.DeepClone().AsBsonDocument, updateBson, isUpsertInsert: false,
                arrayFilters: renderedArrayFilters, matchedArrayIndex: matchedIdx, matchedArrayField: matchedField);
            previewDoc["_id"] = targetId;
            _indexManager.ValidateDocument(previewDoc, excludeId: targetId);
            ValidateSchemaOnWrite(previewDoc);

            // Use DocumentStore.Update for atomic apply and correct Update change type (not Replace)
            var (matched, modified, beforeChange) = _store.Update(targetId, currentDoc =>
            {
                var (curIdx, curField) = renderedFilter != null
                    ? BsonUpdateEvaluator.FindMatchedArrayIndex(currentDoc, renderedFilter)
                    : (-1, (string?)null);
                var applied = ApplyUpdate(currentDoc, updateBson, isUpsertInsert: false,
                    arrayFilters: renderedArrayFilters, matchedArrayIndex: curIdx, matchedArrayField: curField);
                applied["_id"] = targetId;
                return applied;
            });

            if (matched && modified)
            {
                modifiedCount++;
                if (beforeChange != null)
                {
                    var afterDoc = _store.Get(targetId);
                    if (afterDoc != null) PublishChangeEvent(ChangeStreamOperationType.Update, afterDoc, beforeChange);
                }
            }
        }
        return new UpdateResult.Acknowledged(matches.Count, modifiedCount, null);
    }

    public UpdateResult UpdateMany(IClientSessionHandle session, FilterDefinition<TDocument> filter, UpdateDefinition<TDocument> update, UpdateOptions? options = null, CancellationToken cancellationToken = default)
        => UpdateMany(filter, update, options, cancellationToken);

    public Task<UpdateResult> UpdateManyAsync(FilterDefinition<TDocument> filter, UpdateDefinition<TDocument> update, UpdateOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(UpdateMany(filter, update, options, cancellationToken));

    public Task<UpdateResult> UpdateManyAsync(IClientSessionHandle session, FilterDefinition<TDocument> filter, UpdateDefinition<TDocument> update, UpdateOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(UpdateMany(filter, update, options, cancellationToken));

    #endregion

    #region FindOneAnd*

    // Ref: https://www.mongodb.com/docs/manual/reference/command/findAndModify/
    //   "The findAndModify command modifies and returns a single document."

    public TDocument FindOneAndDelete(FilterDefinition<TDocument> filter, FindOneAndDeleteOptions<TDocument, TDocument>? options = null, CancellationToken cancellationToken = default)
    {
        return FindOneAndDelete<TDocument>(filter, options, cancellationToken);
    }

    public TProjection FindOneAndDelete<TProjection>(FilterDefinition<TDocument> filter, FindOneAndDeleteOptions<TDocument, TProjection>? options = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var renderedFilter = RenderFilter(filter);

        if (_commandEventEmitter != null)
        {
            return _commandEventEmitter.EmitCommandEvents(
                "findAndModify",
                CollectionNamespace.DatabaseNamespace,
                () => new BsonDocument { { "findAndModify", CollectionNamespace.CollectionName }, { "query", renderedFilter?.DeepClone() ?? new BsonDocument() }, { "remove", true } },
                () => FindOneAndDeleteCore<TProjection>(filter, options, renderedFilter),
                replyBuilder: result => ReplyHelper.FindAndModifyReply(result != null));
        }

        return FindOneAndDeleteCore<TProjection>(filter, options, renderedFilter);
    }

    private TProjection FindOneAndDeleteCore<TProjection>(FilterDefinition<TDocument> filter, FindOneAndDeleteOptions<TDocument, TProjection>? options, BsonDocument? renderedFilter)
    {
        FaultInjector?.Invoke("findAndModify", renderedFilter);
        OperationLog.Record(new OperationRecord { Type = "FindOneAndDelete", Filter = renderedFilter?.DeepClone().AsBsonDocument });
        var matches = FindInternalBson(filter);

        if (options?.Sort != null)
        {
            var registry = BsonSerializer.SerializerRegistry;
            var renderedSort = options.Sort.Render(new RenderArgs<TDocument>(_serializer, registry));
            if (renderedSort != null && renderedSort.ElementCount > 0)
                matches = BsonSortEvaluator.Apply(matches, renderedSort).ToList();
        }

        if (matches.Count == 0)
            return default!;

        var target = matches[0];
        _store.Remove(target["_id"]);
        PublishChangeEvent(ChangeStreamOperationType.Delete, target);

        var result = ApplyProjectionIfNeeded(target, options?.Projection);
        return BsonSerializer.Deserialize<TProjection>(result);
    }

    public TDocument FindOneAndDelete(IClientSessionHandle session, FilterDefinition<TDocument> filter, FindOneAndDeleteOptions<TDocument, TDocument>? options = null, CancellationToken cancellationToken = default)
        => FindOneAndDelete(filter, options, cancellationToken);

    public TProjection FindOneAndDelete<TProjection>(IClientSessionHandle session, FilterDefinition<TDocument> filter, FindOneAndDeleteOptions<TDocument, TProjection>? options = null, CancellationToken cancellationToken = default)
        => FindOneAndDelete(filter, options, cancellationToken);

    public Task<TDocument> FindOneAndDeleteAsync(FilterDefinition<TDocument> filter, FindOneAndDeleteOptions<TDocument, TDocument>? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(FindOneAndDelete(filter, options, cancellationToken));

    public Task<TProjection> FindOneAndDeleteAsync<TProjection>(FilterDefinition<TDocument> filter, FindOneAndDeleteOptions<TDocument, TProjection>? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(FindOneAndDelete(filter, options, cancellationToken));

    public Task<TDocument> FindOneAndDeleteAsync(IClientSessionHandle session, FilterDefinition<TDocument> filter, FindOneAndDeleteOptions<TDocument, TDocument>? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(FindOneAndDelete(filter, options, cancellationToken));

    public Task<TProjection> FindOneAndDeleteAsync<TProjection>(IClientSessionHandle session, FilterDefinition<TDocument> filter, FindOneAndDeleteOptions<TDocument, TProjection>? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(FindOneAndDelete(filter, options, cancellationToken));

    public TDocument FindOneAndReplace(FilterDefinition<TDocument> filter, TDocument replacement, FindOneAndReplaceOptions<TDocument, TDocument>? options = null, CancellationToken cancellationToken = default)
    {
        return FindOneAndReplace<TDocument>(filter, replacement, options, cancellationToken);
    }

    public TProjection FindOneAndReplace<TProjection>(FilterDefinition<TDocument> filter, TDocument replacement, FindOneAndReplaceOptions<TDocument, TProjection>? options = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var renderedFilter = RenderFilter(filter);

        if (_commandEventEmitter != null)
        {
            return _commandEventEmitter.EmitCommandEvents(
                "findAndModify",
                CollectionNamespace.DatabaseNamespace,
                () => new BsonDocument { { "findAndModify", CollectionNamespace.CollectionName }, { "query", renderedFilter?.DeepClone() ?? new BsonDocument() }, { "update", SerializeDocument(replacement).DeepClone() } },
                () => FindOneAndReplaceCore<TProjection>(filter, replacement, options, renderedFilter),
                replyBuilder: result => ReplyHelper.FindAndModifyReply(result != null));
        }

        return FindOneAndReplaceCore<TProjection>(filter, replacement, options, renderedFilter);
    }

    private TProjection FindOneAndReplaceCore<TProjection>(FilterDefinition<TDocument> filter, TDocument replacement, FindOneAndReplaceOptions<TDocument, TProjection>? options, BsonDocument? renderedFilter)
    {
        FaultInjector?.Invoke("findAndModify", renderedFilter);
        OperationLog.Record(new OperationRecord { Type = "FindOneAndReplace", Filter = renderedFilter?.DeepClone().AsBsonDocument });
        var replacementBson = SerializeDocument(replacement);
        if (replacementBson.Names.Any(n => n.StartsWith("$")))
            throw MongoErrors.BadValue("the replace operation document must not contain atomic operators");

        var matches = FindInternalBson(filter);
        if (options?.Sort != null)
        {
            var registry = BsonSerializer.SerializerRegistry;
            var renderedSort = options.Sort.Render(new RenderArgs<TDocument>(_serializer, registry));
            if (renderedSort != null && renderedSort.ElementCount > 0)
                matches = BsonSortEvaluator.Apply(matches, renderedSort).ToList();
        }

        if (matches.Count == 0)
        {
            if (options?.IsUpsert == true)
            {
                // Ref: https://www.mongodb.com/docs/manual/reference/method/db.collection.findOneAndReplace/
                //   "If upsert: true and no documents match, uses the _id from the replacement
                //    or the equality _id from the filter."
                if (!replacementBson.Contains("_id") || replacementBson["_id"] == BsonNull.Value)
                {
                    var filterDoc = CreateUpsertDocumentFromFilter(filter);
                    if (filterDoc.Contains("_id") && filterDoc["_id"] != BsonNull.Value)
                    {
                        replacementBson.Remove("_id");
                        replacementBson.InsertAt(0, new BsonElement("_id", filterDoc["_id"]));
                    }
                }
                DocumentStore.EnsureId(replacementBson);
                _indexManager.ValidateDocument(replacementBson);
                ValidateSchemaOnWrite(replacementBson);
                var doc = _store.Insert(replacementBson);
                PublishChangeEvent(ChangeStreamOperationType.Insert, doc);
                if (options.ReturnDocument == ReturnDocument.After)
                {
                    var afterResult = ApplyProjectionIfNeeded(doc, options.Projection);
                    return BsonSerializer.Deserialize<TProjection>(afterResult);
                }
                return default!;
            }
            return default!;
        }

        var target = matches[0];
        var targetId = target["_id"];

        // Ref: https://www.mongodb.com/docs/manual/reference/error-codes/
        //   If replacement has an explicit _id that differs from matched doc, error code 66.
        //   A null/missing _id in replacement is fine — we carry over the original _id.
        if (replacementBson.Contains("_id") && replacementBson["_id"] != BsonNull.Value
            && !replacementBson["_id"].Equals(targetId))
            throw MongoErrors.ImmutableField("_id");

        replacementBson["_id"] = targetId;
        _indexManager.ValidateDocument(replacementBson, excludeId: targetId);
        ValidateSchemaOnWrite(replacementBson);
        var (_, wasModified) = _store.Replace(targetId, replacementBson);
        // Ref: https://www.mongodb.com/docs/manual/changeStreams/
        //   Change stream events are only emitted when the document is actually modified.
        if (wasModified)
            PublishChangeEvent(ChangeStreamOperationType.Replace, replacementBson, target);

        // Ref: https://www.mongodb.com/docs/manual/reference/command/findAndModify/
        //   "returnDocument: 'before' returns the original document, 'after' returns the modified document."
        var resultDoc = options?.ReturnDocument == ReturnDocument.After ? replacementBson : target;
        var projected = ApplyProjectionIfNeeded(resultDoc, options?.Projection);
        return BsonSerializer.Deserialize<TProjection>(projected);
    }

    public TDocument FindOneAndReplace(IClientSessionHandle session, FilterDefinition<TDocument> filter, TDocument replacement, FindOneAndReplaceOptions<TDocument, TDocument>? options = null, CancellationToken cancellationToken = default)
        => FindOneAndReplace(filter, replacement, options, cancellationToken);

    public TProjection FindOneAndReplace<TProjection>(IClientSessionHandle session, FilterDefinition<TDocument> filter, TDocument replacement, FindOneAndReplaceOptions<TDocument, TProjection>? options = null, CancellationToken cancellationToken = default)
        => FindOneAndReplace(filter, replacement, options, cancellationToken);

    public Task<TDocument> FindOneAndReplaceAsync(FilterDefinition<TDocument> filter, TDocument replacement, FindOneAndReplaceOptions<TDocument, TDocument>? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(FindOneAndReplace(filter, replacement, options, cancellationToken));

    public Task<TProjection> FindOneAndReplaceAsync<TProjection>(FilterDefinition<TDocument> filter, TDocument replacement, FindOneAndReplaceOptions<TDocument, TProjection>? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(FindOneAndReplace(filter, replacement, options, cancellationToken));

    public Task<TDocument> FindOneAndReplaceAsync(IClientSessionHandle session, FilterDefinition<TDocument> filter, TDocument replacement, FindOneAndReplaceOptions<TDocument, TDocument>? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(FindOneAndReplace(filter, replacement, options, cancellationToken));

    public Task<TProjection> FindOneAndReplaceAsync<TProjection>(IClientSessionHandle session, FilterDefinition<TDocument> filter, TDocument replacement, FindOneAndReplaceOptions<TDocument, TProjection>? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(FindOneAndReplace(filter, replacement, options, cancellationToken));

    public TDocument FindOneAndUpdate(FilterDefinition<TDocument> filter, UpdateDefinition<TDocument> update, FindOneAndUpdateOptions<TDocument, TDocument>? options = null, CancellationToken cancellationToken = default)
    {
        return FindOneAndUpdate<TDocument>(filter, update, options, cancellationToken);
    }

    public TProjection FindOneAndUpdate<TProjection>(FilterDefinition<TDocument> filter, UpdateDefinition<TDocument> update, FindOneAndUpdateOptions<TDocument, TProjection>? options = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var renderedFilter = RenderFilter(filter);

        if (_commandEventEmitter != null)
        {
            return _commandEventEmitter.EmitCommandEvents(
                "findAndModify",
                CollectionNamespace.DatabaseNamespace,
                () => new BsonDocument { { "findAndModify", CollectionNamespace.CollectionName }, { "query", renderedFilter?.DeepClone() ?? new BsonDocument() }, { "update", true } },
                () => FindOneAndUpdateCore<TProjection>(filter, update, options, renderedFilter),
                replyBuilder: result => ReplyHelper.FindAndModifyReply(result != null));
        }

        return FindOneAndUpdateCore<TProjection>(filter, update, options, renderedFilter);
    }

    private TProjection FindOneAndUpdateCore<TProjection>(FilterDefinition<TDocument> filter, UpdateDefinition<TDocument> update, FindOneAndUpdateOptions<TDocument, TProjection>? options, BsonDocument? renderedFilter)
    {
        var updateBson = RenderUpdate(update);
        FaultInjector?.Invoke("findAndModify", renderedFilter);
        OperationLog.Record(new OperationRecord { Type = "FindOneAndUpdate", Filter = renderedFilter?.DeepClone().AsBsonDocument, Update = updateBson is BsonDocument ud ? ud.DeepClone().AsBsonDocument : null });
        ValidateUpdate(updateBson);

        var renderedArrayFilters = RenderArrayFilters(options?.ArrayFilters);

        var matches = FindInternalBson(filter);
        if (options?.Sort != null)
        {
            var registry = BsonSerializer.SerializerRegistry;
            var renderedSort = options.Sort.Render(new RenderArgs<TDocument>(_serializer, registry));
            if (renderedSort != null && renderedSort.ElementCount > 0)
                matches = BsonSortEvaluator.Apply(matches, renderedSort).ToList();
        }

        if (matches.Count == 0)
        {
            if (options?.IsUpsert == true)
            {
                var upsertDoc = updateBson is BsonArray
                    ? CreateUpsertDocumentFromFilter(filter)
                    : CreateUpsertDocument(filter, updateBson.AsBsonDocument);
                var upsertResult = ApplyUpdate(upsertDoc, updateBson, isUpsertInsert: true,
                    arrayFilters: renderedArrayFilters);
                DocumentStore.EnsureId(upsertResult);
                _indexManager.ValidateDocument(upsertResult);
                ValidateSchemaOnWrite(upsertResult);
                var inserted = _store.Insert(upsertResult);
                PublishChangeEvent(ChangeStreamOperationType.Insert, inserted);
                if (options.ReturnDocument == ReturnDocument.After)
                {
                    var afterResult = ApplyProjectionIfNeeded(inserted, options.Projection);
                    return BsonSerializer.Deserialize<TProjection>(afterResult);
                }
                return default!;
            }
            return default!;
        }

        var target = matches[0];
        var targetId = target["_id"];

        var (matchedIdx, matchedField) = renderedFilter != null
            ? BsonUpdateEvaluator.FindMatchedArrayIndex(target, renderedFilter)
            : (-1, (string?)null);

        // Pre-validate: compute the updated doc and check unique indexes before committing
        var previewDoc = ApplyUpdate(target.DeepClone().AsBsonDocument, updateBson,
            arrayFilters: renderedArrayFilters, matchedArrayIndex: matchedIdx, matchedArrayField: matchedField);
        previewDoc["_id"] = targetId;
        _indexManager.ValidateDocument(previewDoc, excludeId: targetId);
        ValidateSchemaOnWrite(previewDoc);

        // Use DocumentStore.Update for atomic apply and correct Update change type
        var (matched, modified, beforeChange) = _store.Update(targetId, currentDoc =>
        {
            var (curIdx, curField) = renderedFilter != null
                ? BsonUpdateEvaluator.FindMatchedArrayIndex(currentDoc, renderedFilter)
                : (-1, (string?)null);
            var applied = ApplyUpdate(currentDoc, updateBson,
                arrayFilters: renderedArrayFilters, matchedArrayIndex: curIdx, matchedArrayField: curField);
            applied["_id"] = targetId;
            return applied;
        });

        if (matched && modified && beforeChange != null)
        {
            var afterDoc = _store.Get(targetId);
            if (afterDoc != null) PublishChangeEvent(ChangeStreamOperationType.Update, afterDoc, beforeChange);
        }

        var resultDoc = options?.ReturnDocument == ReturnDocument.After
            ? (_store.Get(targetId) ?? target)
            : target;
        var projected = ApplyProjectionIfNeeded(resultDoc, options?.Projection);
        return BsonSerializer.Deserialize<TProjection>(projected);
    }

    public TDocument FindOneAndUpdate(IClientSessionHandle session, FilterDefinition<TDocument> filter, UpdateDefinition<TDocument> update, FindOneAndUpdateOptions<TDocument, TDocument>? options = null, CancellationToken cancellationToken = default)
        => FindOneAndUpdate(filter, update, options, cancellationToken);

    public TProjection FindOneAndUpdate<TProjection>(IClientSessionHandle session, FilterDefinition<TDocument> filter, UpdateDefinition<TDocument> update, FindOneAndUpdateOptions<TDocument, TProjection>? options = null, CancellationToken cancellationToken = default)
        => FindOneAndUpdate(filter, update, options, cancellationToken);

    public Task<TDocument> FindOneAndUpdateAsync(FilterDefinition<TDocument> filter, UpdateDefinition<TDocument> update, FindOneAndUpdateOptions<TDocument, TDocument>? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(FindOneAndUpdate(filter, update, options, cancellationToken));

    public Task<TProjection> FindOneAndUpdateAsync<TProjection>(FilterDefinition<TDocument> filter, UpdateDefinition<TDocument> update, FindOneAndUpdateOptions<TDocument, TProjection>? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(FindOneAndUpdate(filter, update, options, cancellationToken));

    public Task<TDocument> FindOneAndUpdateAsync(IClientSessionHandle session, FilterDefinition<TDocument> filter, UpdateDefinition<TDocument> update, FindOneAndUpdateOptions<TDocument, TDocument>? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(FindOneAndUpdate(filter, update, options, cancellationToken));

    public Task<TProjection> FindOneAndUpdateAsync<TProjection>(IClientSessionHandle session, FilterDefinition<TDocument> filter, UpdateDefinition<TDocument> update, FindOneAndUpdateOptions<TDocument, TProjection>? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(FindOneAndUpdate(filter, update, options, cancellationToken));

    #endregion

    #region Aggregate

    // Ref: https://www.mongodb.com/docs/manual/aggregation/
    //   "An aggregation pipeline consists of one or more stages that process documents."
    public IAsyncCursor<TResult> Aggregate<TResult>(PipelineDefinition<TDocument, TResult> pipeline, AggregateOptions? options = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (_commandEventEmitter != null)
        {
            List<BsonDocument>? capturedDocs = null;
            return _commandEventEmitter.EmitCommandEvents(
                "aggregate",
                CollectionNamespace.DatabaseNamespace,
                () => new BsonDocument { { "aggregate", CollectionNamespace.CollectionName }, { "pipeline", new BsonArray() } },
                () =>
                {
                    var cursor = AggregateCore<TResult>(pipeline, options, out var resultDocs);
                    capturedDocs = resultDocs;
                    return cursor;
                },
                replyBuilder: _ => ReplyHelper.CursorReply(capturedDocs ?? [], CollectionNamespace));
        }

        return AggregateCore<TResult>(pipeline, options);
    }

    private IAsyncCursor<TResult> AggregateCore<TResult>(PipelineDefinition<TDocument, TResult> pipeline, AggregateOptions? options)
    {
        return AggregateCore<TResult>(pipeline, options, out _);
    }

    private IAsyncCursor<TResult> AggregateCore<TResult>(PipelineDefinition<TDocument, TResult> pipeline, AggregateOptions? options, out List<BsonDocument> resultDocuments)
    {
        // Render the pipeline to BsonDocument stages
        var registry = BsonSerializer.SerializerRegistry;
        var rendered = pipeline.Render(new RenderArgs<TDocument>(_serializer, registry));
        var stages = rendered.Documents.ToList();

        FaultInjector?.Invoke("aggregate", null);
        OperationLog.Record(new OperationRecord { Type = "Aggregate", Pipeline = stages.Select(s => s.DeepClone().AsBsonDocument).ToArray() });

        // Build aggregation context for cross-collection stages ($lookup, $merge, $out, etc.)
        var db = Database as InMemoryMongoDatabase;
        var client = db?.Client as InMemoryMongoClient;
        var context = new AggregationContext(db!, client)
        {
            CollectionNamespace = CollectionNamespace.FullName,
            DocumentCount = _store.Count
        };

        // Get all documents from this collection as input
        // Ref: https://www.mongodb.com/docs/manual/core/views/
        //   "When clients query a view, MongoDB appends the client query to the underlying pipeline."
        var input = GetStoreDocuments().Select(d => d.DeepClone().AsBsonDocument);

        // Execute the pipeline
        var results = AggregationPipelineExecutor.Execute(input, stages, context).ToList();
        resultDocuments = results;

        // Deserialize results to TResult using the pipeline's output serializer
        var outputSerializer = rendered.OutputSerializer ?? BsonSerializer.LookupSerializer<TResult>();
        var deserialized = results.Select(bson =>
        {
            using var reader = new global::MongoDB.Bson.IO.BsonDocumentReader(bson);
            var context = BsonDeserializationContext.CreateRoot(reader);
            return outputSerializer.Deserialize(context);
        }).ToList();

        return new InMemoryAsyncCursor<TResult>(deserialized);
    }

    public IAsyncCursor<TResult> Aggregate<TResult>(IClientSessionHandle session, PipelineDefinition<TDocument, TResult> pipeline, AggregateOptions? options = null, CancellationToken cancellationToken = default)
        => Aggregate(pipeline, options, cancellationToken);

    public Task<IAsyncCursor<TResult>> AggregateAsync<TResult>(PipelineDefinition<TDocument, TResult> pipeline, AggregateOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(Aggregate(pipeline, options, cancellationToken));

    public Task<IAsyncCursor<TResult>> AggregateAsync<TResult>(IClientSessionHandle session, PipelineDefinition<TDocument, TResult> pipeline, AggregateOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(Aggregate(pipeline, options, cancellationToken));

    public void AggregateToCollection<TResult>(PipelineDefinition<TDocument, TResult> pipeline, AggregateOptions? options = null, CancellationToken cancellationToken = default)
    {
        // Same as Aggregate but discard results (pipeline contains $out or $merge that writes)
        Aggregate(pipeline, options, cancellationToken);
    }

    public void AggregateToCollection<TResult>(IClientSessionHandle session, PipelineDefinition<TDocument, TResult> pipeline, AggregateOptions? options = null, CancellationToken cancellationToken = default)
        => AggregateToCollection(pipeline, options, cancellationToken);

    public Task AggregateToCollectionAsync<TResult>(PipelineDefinition<TDocument, TResult> pipeline, AggregateOptions? options = null, CancellationToken cancellationToken = default)
    {
        AggregateToCollection(pipeline, options, cancellationToken);
        return Task.CompletedTask;
    }

    public Task AggregateToCollectionAsync<TResult>(IClientSessionHandle session, PipelineDefinition<TDocument, TResult> pipeline, AggregateOptions? options = null, CancellationToken cancellationToken = default)
    {
        AggregateToCollection(pipeline, options, cancellationToken);
        return Task.CompletedTask;
    }

    #endregion

    #region Distinct

    // Ref: https://www.mongodb.com/docs/manual/reference/command/distinct/
    //   "Finds the distinct values for a specified field across a single collection."

    public IAsyncCursor<TField> Distinct<TField>(FieldDefinition<TDocument, TField> field, FilterDefinition<TDocument> filter, DistinctOptions? options = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var registry = BsonSerializer.SerializerRegistry;
        var renderedField = field.Render(new RenderArgs<TDocument>(_serializer, registry));
        var fieldName = renderedField.FieldName;

        var matches = FindInternalBson(filter);
        var seen = new HashSet<BsonValue>(BsonValueComparer.Instance);
        var results = new List<TField>();

        foreach (var doc in matches)
        {
            // Ref: https://www.mongodb.com/docs/manual/reference/command/distinct/
            //   "If the value of the specified field is an array, distinct considers each element
            //    of the array as a separate value."
            //   Real MongoDB does NOT include null for documents where the field is missing.
            //   Only explicitly null-valued fields contribute a null value.
            // Ref: https://www.mongodb.com/docs/manual/core/document/#dot-notation
            //   Dot-notation traverses arrays of subdocuments.
            if (!fieldName.Contains('.') && !BsonFilterEvaluator.FieldExists(doc, fieldName))
                continue;

            var allValues = fieldName.Contains('.')
                ? BsonFilterEvaluator.ResolveFieldPathThroughArrays(doc, fieldName)
                : new List<BsonValue> { BsonFilterEvaluator.ResolveFieldPath(doc, fieldName) };

            foreach (var val in allValues)
            {
                var elements = val is BsonArray arr ? arr.Values : new[] { val };
                foreach (var element in elements)
                {
                    if (!seen.Add(element)) continue;

                    if (typeof(TField) == typeof(BsonValue))
                        results.Add((TField)(object)element);
                    else if (element == BsonNull.Value || element.IsBsonNull)
                        results.Add(default!);
                    else
                        results.Add((TField)BsonTypeMapper.MapToDotNetValue(element));
                }
            }
        }

        return new InMemoryAsyncCursor<TField>(results, results.Count + 1);
    }

    public IAsyncCursor<TField> Distinct<TField>(IClientSessionHandle session, FieldDefinition<TDocument, TField> field, FilterDefinition<TDocument> filter, DistinctOptions? options = null, CancellationToken cancellationToken = default)
        => Distinct(field, filter, options, cancellationToken);

    public Task<IAsyncCursor<TField>> DistinctAsync<TField>(FieldDefinition<TDocument, TField> field, FilterDefinition<TDocument> filter, DistinctOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(Distinct(field, filter, options, cancellationToken));

    public Task<IAsyncCursor<TField>> DistinctAsync<TField>(IClientSessionHandle session, FieldDefinition<TDocument, TField> field, FilterDefinition<TDocument> filter, DistinctOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(Distinct(field, filter, options, cancellationToken));

    public IAsyncCursor<TItem> DistinctMany<TItem>(FieldDefinition<TDocument, IEnumerable<TItem>> field, FilterDefinition<TDocument> filter, DistinctOptions? options = null, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("DistinctMany is not yet implemented.");
    }

    public IAsyncCursor<TItem> DistinctMany<TItem>(IClientSessionHandle session, FieldDefinition<TDocument, IEnumerable<TItem>> field, FilterDefinition<TDocument> filter, DistinctOptions? options = null, CancellationToken cancellationToken = default)
        => DistinctMany(field, filter, options, cancellationToken);

    public Task<IAsyncCursor<TItem>> DistinctManyAsync<TItem>(FieldDefinition<TDocument, IEnumerable<TItem>> field, FilterDefinition<TDocument> filter, DistinctOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(DistinctMany(field, filter, options, cancellationToken));

    public Task<IAsyncCursor<TItem>> DistinctManyAsync<TItem>(IClientSessionHandle session, FieldDefinition<TDocument, IEnumerable<TItem>> field, FilterDefinition<TDocument> filter, DistinctOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(DistinctMany(field, filter, options, cancellationToken));

    #endregion

    #region BulkWrite

    // Ref: https://www.mongodb.com/docs/manual/reference/command/bulkWrite/
    //   "Performs multiple write operations with controls for order of execution."

    public BulkWriteResult<TDocument> BulkWrite(IEnumerable<WriteModel<TDocument>> requests, BulkWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var requestList = requests.ToList();
        var isOrdered = options?.IsOrdered ?? true;

        long insertedCount = 0;
        long matchedCount = 0;
        long modifiedCount = 0;
        long deletedCount = 0;
        var upserts = new List<BulkWriteUpsert>();
        var processedRequests = new List<WriteModel<TDocument>>();
        var errors = new List<BulkWriteError>();

        for (int i = 0; i < requestList.Count; i++)
        {
            try
            {
                var request = requestList[i];
                processedRequests.Add(request);

                switch (request)
                {
                    case InsertOneModel<TDocument> insert:
                        var bson = SerializeDocument(insert.Document);
                        DocumentStore.EnsureId(bson);
                        _indexManager.ValidateDocument(bson);
                        ValidateSchemaOnWrite(bson);
                        var insertedDoc = _store.Insert(bson);
                        PublishChangeEvent(ChangeStreamOperationType.Insert, insertedDoc);
                        WriteBackId(insert.Document, bson);
                        insertedCount++;
                        break;

                    case UpdateOneModel<TDocument> updateOne:
                        var updateResult = UpdateOne(updateOne.Filter, updateOne.Update,
                            new UpdateOptions { IsUpsert = updateOne.IsUpsert, ArrayFilters = updateOne.ArrayFilters }, cancellationToken);
                        matchedCount += updateResult.MatchedCount;
                        modifiedCount += updateResult.ModifiedCount;
                        if (updateResult.UpsertedId != null)
                            upserts.Add(MongoErrors.CreateBulkWriteUpsert(i, updateResult.UpsertedId));
                        break;

                    case UpdateManyModel<TDocument> updateMany:
                        var updateManyResult = UpdateMany(updateMany.Filter, updateMany.Update,
                            new UpdateOptions { IsUpsert = updateMany.IsUpsert, ArrayFilters = updateMany.ArrayFilters }, cancellationToken);
                        matchedCount += updateManyResult.MatchedCount;
                        modifiedCount += updateManyResult.ModifiedCount;
                        if (updateManyResult.UpsertedId != null)
                            upserts.Add(MongoErrors.CreateBulkWriteUpsert(i, updateManyResult.UpsertedId));
                        break;

                    case DeleteOneModel<TDocument> deleteOne:
                        var delOneResult = DeleteOne(deleteOne.Filter, cancellationToken: cancellationToken);
                        deletedCount += delOneResult.DeletedCount;
                        break;

                    case DeleteManyModel<TDocument> deleteMany:
                        var delManyResult = DeleteMany(deleteMany.Filter, cancellationToken: cancellationToken);
                        deletedCount += delManyResult.DeletedCount;
                        break;

                    case ReplaceOneModel<TDocument> replaceOne:
                        var replaceResult = ReplaceOne(replaceOne.Filter, replaceOne.Replacement,
                            new ReplaceOptions { IsUpsert = replaceOne.IsUpsert }, cancellationToken);
                        matchedCount += replaceResult.MatchedCount;
                        modifiedCount += replaceResult.ModifiedCount;
                        if (replaceResult.UpsertedId != null)
                            upserts.Add(MongoErrors.CreateBulkWriteUpsert(i, replaceResult.UpsertedId));
                        break;
                }
            }
            // Ref: https://www.mongodb.com/docs/drivers/csharp/current/fundamentals/crud/write-operations/bulk-write/
            //   "BulkWrite always throws MongoBulkWriteException on failure, for both ordered and unordered mode."
            catch (Exception ex) when (!isOrdered && ex is MongoWriteException or MongoCommandException)
            {
                // Ref: https://www.mongodb.com/docs/manual/reference/method/db.collection.bulkWrite/
                //   "unordered operations that result in errors will still report the errors."
                var code = ex is MongoWriteException mwe ? mwe.WriteError.Code : 2;
                var category = ex is MongoWriteException mwe2 ? mwe2.WriteError.Category : ServerErrorCategory.Uncategorized;
                errors.Add(MongoErrors.CreateBulkWriteError(i, category, code, ex.Message));
            }
            catch (Exception ex) when (isOrdered && ex is MongoWriteException or MongoCommandException)
            {
                // Ref: https://www.mongodb.com/docs/drivers/csharp/current/fundamentals/crud/write-operations/bulk-write/
                //   "If an error occurs during an ordered bulk operation, the driver throws a
                //    MongoBulkWriteException and does not execute the remaining operations."
                var code = ex is MongoWriteException mwe ? mwe.WriteError.Code : 2;
                var category = ex is MongoWriteException mwe2 ? mwe2.WriteError.Category : ServerErrorCategory.Uncategorized;
                errors.Add(MongoErrors.CreateBulkWriteError(i, category, code, ex.Message));
                break; // ordered mode: stop at first error
            }
        }

        if (errors.Count > 0)
        {
            // Compute unprocessed requests (those after the last processed)
            var unprocessed = requestList.Skip(processedRequests.Count).ToList();

            throw new MongoBulkWriteException<TDocument>(
                MongoErrors.SyntheticConnectionId,
                result: new BulkWriteResult<TDocument>.Acknowledged(
                    requestCount: processedRequests.Count,
                    matchedCount: matchedCount,
                    deletedCount: deletedCount,
                    insertedCount: insertedCount,
                    modifiedCount: modifiedCount,
                    processedRequests: processedRequests,
                    upserts: upserts),
                writeErrors: errors,
                writeConcernError: null,
                unprocessedRequests: unprocessed);
        }

        return MongoErrors.CreateBulkWriteResult<TDocument>(
            requestCount: processedRequests.Count,
            insertedCount: insertedCount,
            matchedCount: matchedCount,
            modifiedCount: modifiedCount,
            deletedCount: deletedCount,
            upserts: upserts,
            processedRequests: processedRequests);
    }

    public BulkWriteResult<TDocument> BulkWrite(IClientSessionHandle session, IEnumerable<WriteModel<TDocument>> requests, BulkWriteOptions? options = null, CancellationToken cancellationToken = default)
        => BulkWrite(requests, options, cancellationToken);

    public Task<BulkWriteResult<TDocument>> BulkWriteAsync(IEnumerable<WriteModel<TDocument>> requests, BulkWriteOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(BulkWrite(requests, options, cancellationToken));

    public Task<BulkWriteResult<TDocument>> BulkWriteAsync(IClientSessionHandle session, IEnumerable<WriteModel<TDocument>> requests, BulkWriteOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(BulkWrite(requests, options, cancellationToken));

    #endregion

    #region Watch

    public IChangeStreamCursor<TResult> Watch<TResult>(PipelineDefinition<ChangeStreamDocument<TDocument>, TResult> pipeline, ChangeStreamOptions? options = null, CancellationToken cancellationToken = default)
    {
        var client = (Database as InMemoryMongoDatabase)?.Client as InMemoryMongoClient
            ?? throw new NotSupportedException("Watch requires InMemoryMongoClient.");
        var registry = BsonSerializer.SerializerRegistry;
        var outputSerializer = registry.GetSerializer<TResult>();
        return new InMemoryChangeStreamCursor<TResult>(
            client.ChangeNotifier,
            databaseFilter: CollectionNamespace.DatabaseNamespace.DatabaseName,
            collectionFilter: CollectionNamespace.CollectionName,
            options,
            outputSerializer,
            startSequence: client.ChangeNotifier.CurrentSequence);
    }

    public IChangeStreamCursor<TResult> Watch<TResult>(IClientSessionHandle session, PipelineDefinition<ChangeStreamDocument<TDocument>, TResult> pipeline, ChangeStreamOptions? options = null, CancellationToken cancellationToken = default)
        => Watch(pipeline, options, cancellationToken);

    public Task<IChangeStreamCursor<TResult>> WatchAsync<TResult>(PipelineDefinition<ChangeStreamDocument<TDocument>, TResult> pipeline, ChangeStreamOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(Watch(pipeline, options, cancellationToken));

    public Task<IChangeStreamCursor<TResult>> WatchAsync<TResult>(IClientSessionHandle session, PipelineDefinition<ChangeStreamDocument<TDocument>, TResult> pipeline, ChangeStreamOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(Watch(pipeline, options, cancellationToken));

    #endregion

    #region MapReduce (deprecated)

    [Obsolete("MapReduce is deprecated since MongoDB 5.0. Use Aggregate() instead.")]
    public IAsyncCursor<TResult> MapReduce<TResult>(BsonJavaScript map, BsonJavaScript reduce, MapReduceOptions<TDocument, TResult>? options = null, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException("MapReduce is deprecated since MongoDB 5.0. Use the Aggregate() pipeline instead.");
    }

    [Obsolete("MapReduce is deprecated since MongoDB 5.0. Use Aggregate() instead.")]
    public IAsyncCursor<TResult> MapReduce<TResult>(IClientSessionHandle session, BsonJavaScript map, BsonJavaScript reduce, MapReduceOptions<TDocument, TResult>? options = null, CancellationToken cancellationToken = default)
        => MapReduce(map, reduce, options, cancellationToken);

    [Obsolete("MapReduce is deprecated since MongoDB 5.0. Use Aggregate() instead.")]
    public Task<IAsyncCursor<TResult>> MapReduceAsync<TResult>(BsonJavaScript map, BsonJavaScript reduce, MapReduceOptions<TDocument, TResult>? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(MapReduce(map, reduce, options, cancellationToken));

    [Obsolete("MapReduce is deprecated since MongoDB 5.0. Use Aggregate() instead.")]
    public Task<IAsyncCursor<TResult>> MapReduceAsync<TResult>(IClientSessionHandle session, BsonJavaScript map, BsonJavaScript reduce, MapReduceOptions<TDocument, TResult>? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(MapReduce(map, reduce, options, cancellationToken));

    #endregion

    #region WithReadConcern / WithWriteConcern / WithReadPreference

    // Ref: https://www.mongodb.com/docs/manual/reference/read-concern/
    //   Read/Write concern and read preference stored but not enforced in in-memory.

    public IMongoCollection<TDocument> WithReadConcern(ReadConcern readConcern)
    {
        var settings = Settings.Clone();
        settings.ReadConcern = readConcern;
        return new InMemoryMongoCollection<TDocument>(CollectionNamespace, Database, _store, settings);
    }

    public IMongoCollection<TDocument> WithWriteConcern(WriteConcern writeConcern)
    {
        var settings = Settings.Clone();
        settings.WriteConcern = writeConcern;
        return new InMemoryMongoCollection<TDocument>(CollectionNamespace, Database, _store, settings);
    }

    public IMongoCollection<TDocument> WithReadPreference(ReadPreference readPreference)
    {
        var settings = Settings.Clone();
        settings.ReadPreference = readPreference;
        return new InMemoryMongoCollection<TDocument>(CollectionNamespace, Database, _store, settings);
    }

    #endregion

    #region OfType

    public IFilteredMongoCollection<TDerivedDocument> OfType<TDerivedDocument>() where TDerivedDocument : TDocument
    {
        throw new NotSupportedException("OfType is not yet implemented. Coming in Phase 3.");
    }

    #endregion

    #region Internal helpers

    /// <summary>
    /// Removes TTL-expired documents from the store (lazy eviction on read).
    /// </summary>
    private void EvictTtlExpiredDocuments()
    {
        var allDocs = _store.GetAll();
        foreach (var doc in allDocs)
        {
            if (_indexManager.IsExpiredByTtl(doc) && doc.Contains("_id"))
            {
                _store.Remove(doc["_id"]);
            }
        }
    }

    private BsonDocument SerializeDocument(TDocument document)
    {
        return document switch
        {
            BsonDocument bson => bson,
            _ => document.ToBsonDocument(_serializer)
        };
    }

    internal TDocument DeserializeDocument(BsonDocument bson)
    {
        if (typeof(TDocument) == typeof(BsonDocument))
            return (TDocument)(object)bson;
        return BsonSerializer.Deserialize<TDocument>(bson);
    }

    private List<TDocument> FindInternal(FilterDefinition<TDocument> filter, FindOptions<TDocument, TDocument>? options)
    {
        var bsonResults = FindInternalBsonFull(filter, options?.Sort, options?.Skip, options?.Limit);
        return bsonResults.Select(DeserializeDocument).ToList();
    }

    /// <summary>
    /// Core find pipeline: filter -> sort -> skip -> limit. Returns raw BsonDocuments.
    /// </summary>
    private List<BsonDocument> FindInternalBsonFull(
        FilterDefinition<TDocument> filter,
        SortDefinition<TDocument>? sort = null,
        int? skip = null,
        int? limit = null)
    {
        var allDocs = GetStoreDocuments();
        var rendered = RenderFilter(filter);

        IEnumerable<BsonDocument> results = allDocs;

        // Apply filter
        if (rendered != null && rendered.ElementCount > 0)
            results = results.Where(doc => BsonFilterEvaluator.Matches(doc, rendered));

        // Ref: https://www.mongodb.com/docs/manual/reference/command/find/
        //   "sort: Optional. The sort specification for the ordering of the results."
        if (sort != null)
        {
            var registry = BsonSerializer.SerializerRegistry;
            var renderedSort = sort.Render(new RenderArgs<TDocument>(_serializer, registry));
            if (renderedSort != null && renderedSort.ElementCount > 0)
                results = BsonSortEvaluator.Apply(results.ToList(), renderedSort);
        }

        // Ref: https://www.mongodb.com/docs/manual/reference/command/find/
        //   "skip: Optional. Number of documents to skip."
        if (skip.HasValue)
            results = results.Skip(skip.Value);

        // Ref: https://www.mongodb.com/docs/manual/reference/command/find/
        //   "limit: Optional. The maximum number of documents to return."
        //   "A limit value of 0 is equivalent to setting no limit."
        if (limit.HasValue && limit.Value > 0)
            results = results.Take(limit.Value);

        return results.ToList();
    }

    private List<BsonDocument> FindInternalBson(FilterDefinition<TDocument> filter)
    {
        return FindInternalBsonFull(filter);
    }

    internal BsonDocument? RenderFilter(FilterDefinition<TDocument> filter)
    {
        if (filter == null || filter == FilterDefinition<TDocument>.Empty)
            return null;

        var registry = BsonSerializer.SerializerRegistry;
        var rendered = filter.Render(new RenderArgs<TDocument>(_serializer, registry));
        return rendered;
    }

    /// <summary>
    /// Gets documents from the store, applying TTL eviction and the view pipeline if applicable.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/core/index-ttl/#expiration-of-data
    ///   "TTL indexes expire documents after the specified number of seconds has passed
    ///    since the indexed field value."
    /// Lazy eviction: expired documents are removed from the store on query.
    /// </remarks>
    private IReadOnlyList<BsonDocument> GetStoreDocuments()
    {
        // TTL lazy eviction: remove expired docs from the store on read
        EvictTtlExpiredDocuments();

        var allDocs = _store.GetAll();
        if (_viewPipeline == null || _viewPipeline.Count == 0) return allDocs;

        // Ref: https://www.mongodb.com/docs/manual/core/views/
        //   "When clients query a view, MongoDB appends the client query to the underlying pipeline."
        var db = Database as InMemoryMongoDatabase;
        var client = db?.Client as InMemoryMongoClient;
        var context = new AggregationContext(db!, client)
        {
            CollectionNamespace = CollectionNamespace.FullName
        };
        return AggregationPipelineExecutor.Execute(allDocs, _viewPipeline, context).ToList();
    }

    internal BsonValue RenderUpdate(UpdateDefinition<TDocument> update)
    {
        var registry = BsonSerializer.SerializerRegistry;
        var rendered = update.Render(new RenderArgs<TDocument>(_serializer, registry));
        return rendered;
    }

    /// <summary>
    /// Applies an update (traditional $set/$inc or pipeline) to a document.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/method/db.collection.updateOne/
    ///   "Starting in MongoDB 4.2, the method can accept an aggregation pipeline."
    /// </remarks>
    private BsonDocument ApplyUpdate(BsonDocument target, BsonValue updateBson, bool isUpsertInsert = false,
        IReadOnlyList<BsonDocument>? arrayFilters = null, int matchedArrayIndex = -1, string? matchedArrayField = null)
    {
        if (updateBson is BsonArray pipelineStages)
        {
            // Pipeline update: apply aggregation stages to the single document
            var stages = pipelineStages.Select(s => s.AsBsonDocument).ToList();
            var db = Database as InMemoryMongoDatabase;
            var client = db?.Client as InMemoryMongoClient;
            var context = new AggregationContext(db!, client)
            {
                CollectionNamespace = CollectionNamespace.FullName,
                DocumentCount = _store.Count
            };
            var result = AggregationPipelineExecutor.Execute(
                new[] { target.DeepClone().AsBsonDocument }, stages, context).ToList();
            return result.Count > 0 ? result[0] : target;
        }

        return BsonUpdateEvaluator.Apply(target, updateBson.AsBsonDocument,
            arrayFilters: arrayFilters, isUpsertInsert: isUpsertInsert,
            matchedArrayIndex: matchedArrayIndex, matchedArrayField: matchedArrayField);
    }

    /// <summary>
    /// Renders ArrayFilterDefinitions from UpdateOptions into BsonDocuments.
    /// </summary>
    private IReadOnlyList<BsonDocument>? RenderArrayFilters(IEnumerable<ArrayFilterDefinition>? arrayFilters)
    {
        if (arrayFilters == null) return null;
        var registry = BsonSerializer.SerializerRegistry;
        return arrayFilters
            .Select(af => af.Render(registry.GetSerializer<BsonDocument>(), registry, LinqProvider.V3))
            .ToList();
    }

    /// <summary>
    /// Validates the update definition (must contain atomic operators if not a pipeline).
    /// </summary>
    private static void ValidateUpdate(BsonValue updateBson)
    {
        if (updateBson is BsonArray) return; // Pipeline update — valid by definition
        BsonUpdateEvaluator.ValidateIsUpdateDocument(updateBson.AsBsonDocument);
    }

    /// <summary>Publish a change event to the change stream notifier (if available).</summary>
    private void PublishChangeEvent(ChangeStreamOperationType opType, BsonDocument doc, BsonDocument? beforeChange = null)
    {
        var client = (Database as InMemoryMongoDatabase)?.Client as InMemoryMongoClient;
        // Ref: https://www.mongodb.com/docs/manual/reference/change-events/delete/
        //   For delete events, fullDocument is omitted and fullDocumentBeforeChange holds the pre-delete state.
        var fullDoc = opType == ChangeStreamOperationType.Delete ? null : doc.DeepClone().AsBsonDocument;
        var fullBefore = opType == ChangeStreamOperationType.Delete
            ? doc.DeepClone().AsBsonDocument
            : beforeChange?.DeepClone().AsBsonDocument;
        client?.ChangeNotifier.Publish(new ChangeEvent
        {
            OperationType = opType,
            DatabaseName = CollectionNamespace.DatabaseNamespace.DatabaseName,
            CollectionName = CollectionNamespace.CollectionName,
            DocumentKey = new BsonDocument("_id", doc.GetValue("_id", BsonNull.Value)),
            FullDocument = fullDoc,
            FullDocumentBeforeChange = fullBefore,
        });
    }

    private BsonDocument ApplyProjectionIfNeeded<TProjection>(BsonDocument doc, ProjectionDefinition<TDocument, TProjection>? projection)
    {
        if (projection == null) return doc;
        var registry = BsonSerializer.SerializerRegistry;
        var rendered = projection.Render(new RenderArgs<TDocument>(_serializer, registry));
        if (rendered.Document == null || rendered.Document.ElementCount == 0) return doc;
        return BsonProjectionEvaluator.Apply(doc, rendered.Document);
    }

    /// <summary>
    /// Creates a base document for an upsert by extracting equality conditions from the filter.
    /// The update operators are NOT applied here — the caller is responsible for applying them
    /// via <see cref="ApplyUpdate"/> exactly once.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/method/db.collection.update/#std-label-upsert-behavior
    ///   "If no document matches the query criteria, update() inserts a single document
    ///    consisting of the equality clause(s) of the query filter, the update modifications, and $setOnInsert."
    /// </remarks>
    private BsonDocument CreateUpsertDocument(FilterDefinition<TDocument> filter, BsonDocument updateBson)
    {
        return CreateUpsertDocumentFromFilter(filter);
    }

    /// <summary>
    /// Creates a base upsert document from filter equality conditions only (for pipeline updates).
    /// </summary>
    private BsonDocument CreateUpsertDocumentFromFilter(FilterDefinition<TDocument> filter)
    {
        var doc = new BsonDocument();

        // Extract equality conditions from filter
        var rendered = RenderFilter(filter);
        if (rendered != null)
        {
            ExtractEqualityConditions(rendered, doc);
        }

        DocumentStore.EnsureId(doc);
        return doc;
    }

    /// <summary>
    /// Recursively extracts equality conditions from a rendered filter into the target document.
    /// Handles $and, direct equality, $eq operator, and document/array equality values.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/method/db.collection.updateOne/
    ///   "If a filter matches no documents and upsert: true, the new document is created
    ///    from the equality conditions in the filter."
    /// </remarks>
    private static void ExtractEqualityConditions(BsonDocument filter, BsonDocument target)
    {
        foreach (var element in filter)
        {
            if (element.Name == "$and" && element.Value is BsonArray andArray)
            {
                // Recurse into $and conditions
                foreach (var sub in andArray)
                {
                    if (sub is BsonDocument subDoc)
                        ExtractEqualityConditions(subDoc, target);
                }
            }
            else if (!element.Name.StartsWith("$"))
            {
                if (element.Value.IsBsonDocument)
                {
                    var inner = element.Value.AsBsonDocument;
                    // Check if it's { $eq: value }
                    if (inner.ElementCount == 1 && inner.GetElement(0).Name == "$eq")
                        target[element.Name] = inner["$eq"];
                    // Document equality: { field: { a: 1, b: 2 } } where no keys start with $
                    else if (inner.ElementCount > 0 && !inner.Names.Any(n => n.StartsWith("$")))
                        target[element.Name] = element.Value;
                    // Skip filter operator docs like { $gt: 5 } — not equality
                }
                else
                {
                    // Direct equality: { field: value } — includes scalars, arrays, and null
                    target[element.Name] = element.Value;
                }
            }
        }
    }

    /// <summary>
    /// Writes the generated _id value back to the source document.
    /// The real MongoDB driver does this automatically during InsertOne.
    /// </summary>
    /// <summary>
    /// Validates a document against the collection's schema validator.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/core/schema-validation/
    ///   "MongoDB can apply validation rules during inserts and updates."
    /// </remarks>
    private void ValidateSchemaOnWrite(BsonDocument doc)
    {
        if (Database is InMemoryMongoDatabase db)
        {
            db.ValidateDocument(CollectionNamespace.CollectionName, doc);
        }
    }

    private void WriteBackId(TDocument document, BsonDocument bson)
    {
        if (document is BsonDocument) return; // BsonDocument already has the _id

        var idProvider = _serializer as IBsonIdProvider;
        if (idProvider != null)
        {
            idProvider.GetDocumentId(document, out var existingId, out var idNominalType, out var idGenerator);
            if (existingId == null || existingId.Equals(default))
            {
                var bsonId = bson["_id"];
                // Convert the BsonValue to the appropriate CLR type
                object? clrId = bsonId.BsonType switch
                {
                    BsonType.ObjectId => bsonId.AsObjectId.ToString(),
                    BsonType.String => bsonId.AsString,
                    BsonType.Int32 => bsonId.AsInt32,
                    BsonType.Int64 => bsonId.AsInt64,
                    _ => BsonTypeMapper.MapToDotNetValue(bsonId)
                };
                idProvider.SetDocumentId(document, clrId);
            }
        }
    }

    #endregion
}
