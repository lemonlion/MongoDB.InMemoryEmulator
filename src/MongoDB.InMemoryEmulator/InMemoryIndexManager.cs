using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace MongoDB.InMemoryEmulator;

/// <summary>
/// In-memory implementation of <see cref="IMongoIndexManager{TDocument}"/>.
/// Stores index metadata, enforces unique constraints, and tracks TTL indexes.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/indexes/
///   "Indexes support the efficient execution of queries in MongoDB."
///   "MongoDB creates a unique index on the _id field during the creation of a collection."
/// </remarks>
public class InMemoryIndexManager<TDocument> : IMongoIndexManager<TDocument>
{
    private readonly IMongoCollection<TDocument> _collection;
    private readonly DocumentStore _store;
    private readonly List<BsonDocument> _indexes = new();
    private readonly object _lock = new();

    internal InMemoryIndexManager(IMongoCollection<TDocument> collection, DocumentStore store)
    {
        _collection = collection;
        _store = store;

        // Default _id index always present
        _indexes.Add(new BsonDocument
        {
            { "v", 2 },
            { "key", new BsonDocument("_id", 1) },
            { "name", "_id_" },
            { "ns", collection.CollectionNamespace.FullName }
        });
    }

    public CollectionNamespace CollectionNamespace => _collection.CollectionNamespace;

    public IBsonSerializer<TDocument> DocumentSerializer => BsonSerializer.LookupSerializer<TDocument>();

    public MongoCollectionSettings Settings => new MongoCollectionSettings();

    public string CreateOne(CreateIndexModel<TDocument> model, CreateOneIndexOptions? options = null, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var indexName = GetOrGenerateIndexName(model);

        lock (_lock)
        {
            // Don't duplicate
            if (_indexes.Any(idx => idx["name"].AsString == indexName))
                return indexName;

            var rendered = model.Keys.Render(new RenderArgs<TDocument>(
                BsonSerializer.LookupSerializer<TDocument>(),
                BsonSerializer.SerializerRegistry));

            var indexDoc = new BsonDocument
            {
                { "v", 2 },
                { "key", rendered },
                { "name", indexName },
                { "ns", _collection.CollectionNamespace.FullName }
            };

            if (model.Options?.Unique == true)
                indexDoc["unique"] = true;
            if (model.Options?.Sparse == true)
                indexDoc["sparse"] = true;
            if (model.Options?.ExpireAfter.HasValue == true)
                indexDoc["expireAfterSeconds"] = (int)model.Options.ExpireAfter.Value.TotalSeconds;

            // Ref: https://www.mongodb.com/docs/manual/core/index-partial/
            //   "Partial indexes only index the documents in a collection that meet a specified filter expression."
            if (model.Options is CreateIndexOptions<TDocument> typedOptions && typedOptions.PartialFilterExpression != null)
            {
                var renderedFilter = typedOptions.PartialFilterExpression.Render(new RenderArgs<TDocument>(
                    BsonSerializer.LookupSerializer<TDocument>(),
                    BsonSerializer.SerializerRegistry));
                indexDoc["partialFilterExpression"] = renderedFilter;
            }

            // Ref: https://www.mongodb.com/docs/manual/core/index-unique/#restrictions
            //   "MongoDB cannot create a unique index on the specified index field(s) if the collection
            //    already contains data that would violate the unique constraint for the index."
            if (model.Options?.Unique == true)
            {
                ValidateExistingDocuments(indexDoc);
            }

            _indexes.Add(indexDoc);
        }

        return indexName;
    }

    public string CreateOne(IClientSessionHandle session, CreateIndexModel<TDocument> model, CreateOneIndexOptions? options = null, CancellationToken cancellationToken = default)
        => CreateOne(model, options, cancellationToken);

    public string CreateOne(IndexKeysDefinition<TDocument> keys, CreateIndexOptions? options = null, CancellationToken cancellationToken = default)
        => CreateOne(new CreateIndexModel<TDocument>(keys, options), cancellationToken: cancellationToken);

    public string CreateOne(IClientSessionHandle session, IndexKeysDefinition<TDocument> keys, CreateIndexOptions? options = null, CancellationToken cancellationToken = default)
        => CreateOne(new CreateIndexModel<TDocument>(keys, options), cancellationToken: cancellationToken);

    public Task<string> CreateOneAsync(CreateIndexModel<TDocument> model, CreateOneIndexOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(CreateOne(model, options, cancellationToken));

    public Task<string> CreateOneAsync(IClientSessionHandle session, CreateIndexModel<TDocument> model, CreateOneIndexOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(CreateOne(model, options, cancellationToken));

    public Task<string> CreateOneAsync(IndexKeysDefinition<TDocument> keys, CreateIndexOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(CreateOne(keys, options, cancellationToken));

    public Task<string> CreateOneAsync(IClientSessionHandle session, IndexKeysDefinition<TDocument> keys, CreateIndexOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(CreateOne(keys, options, cancellationToken));

    public IEnumerable<string> CreateMany(IEnumerable<CreateIndexModel<TDocument>> models, CreateManyIndexesOptions? options = null, CancellationToken cancellationToken = default)
    {
        return models.Select(m => CreateOne(m, cancellationToken: cancellationToken)).ToList();
    }

    public IEnumerable<string> CreateMany(IEnumerable<CreateIndexModel<TDocument>> models, CancellationToken cancellationToken = default)
        => CreateMany(models, options: null, cancellationToken);

    public IEnumerable<string> CreateMany(IClientSessionHandle session, IEnumerable<CreateIndexModel<TDocument>> models, CreateManyIndexesOptions? options = null, CancellationToken cancellationToken = default)
        => CreateMany(models, options, cancellationToken);

    public IEnumerable<string> CreateMany(IClientSessionHandle session, IEnumerable<CreateIndexModel<TDocument>> models, CancellationToken cancellationToken = default)
        => CreateMany(models, options: null, cancellationToken);

    public Task<IEnumerable<string>> CreateManyAsync(IEnumerable<CreateIndexModel<TDocument>> models, CreateManyIndexesOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(CreateMany(models, options, cancellationToken));

    public Task<IEnumerable<string>> CreateManyAsync(IEnumerable<CreateIndexModel<TDocument>> models, CancellationToken cancellationToken = default)
        => Task.FromResult(CreateMany(models, cancellationToken));

    public Task<IEnumerable<string>> CreateManyAsync(IClientSessionHandle session, IEnumerable<CreateIndexModel<TDocument>> models, CreateManyIndexesOptions? options = null, CancellationToken cancellationToken = default)
        => Task.FromResult(CreateMany(models, options, cancellationToken));

    public Task<IEnumerable<string>> CreateManyAsync(IClientSessionHandle session, IEnumerable<CreateIndexModel<TDocument>> models, CancellationToken cancellationToken = default)
        => Task.FromResult(CreateMany(models, cancellationToken));

    public void DropAll(DropIndexOptions? options, CancellationToken cancellationToken = default)
    {
        lock (_lock)
        {
            _indexes.RemoveAll(idx => idx["name"].AsString != "_id_");
        }
    }

    public void DropAll(CancellationToken cancellationToken = default)
        => DropAll(options: null, cancellationToken);

    public void DropAll(IClientSessionHandle session, DropIndexOptions? options = null, CancellationToken cancellationToken = default)
        => DropAll(options, cancellationToken);

    public void DropAll(IClientSessionHandle session, CancellationToken cancellationToken = default)
        => DropAll(options: null, cancellationToken);

    public Task DropAllAsync(DropIndexOptions? options, CancellationToken cancellationToken = default)
    {
        DropAll(options, cancellationToken);
        return Task.CompletedTask;
    }

    public Task DropAllAsync(CancellationToken cancellationToken = default)
        => DropAllAsync(options: null, cancellationToken);

    public Task DropAllAsync(IClientSessionHandle session, DropIndexOptions? options = null, CancellationToken cancellationToken = default)
        => DropAllAsync(options, cancellationToken);

    public Task DropAllAsync(IClientSessionHandle session, CancellationToken cancellationToken = default)
        => DropAllAsync(options: null, cancellationToken);

    public void DropOne(string name, CancellationToken cancellationToken = default)
        => DropOne(name, options: null, cancellationToken);

    public void DropOne(string name, DropIndexOptions? options, CancellationToken cancellationToken = default)
    {
        if (name == "_id_")
            throw new MongoCommandException(null!, "cannot drop _id index", new BsonDocument("ok", 0));

        lock (_lock)
        {
            _indexes.RemoveAll(idx => idx["name"].AsString == name);
        }
    }

    public void DropOne(IClientSessionHandle session, string name, DropIndexOptions? options = null, CancellationToken cancellationToken = default)
        => DropOne(name, options, cancellationToken);

    public void DropOne(IClientSessionHandle session, string name, CancellationToken cancellationToken = default)
        => DropOne(name, cancellationToken: cancellationToken);

    public Task DropOneAsync(string name, CancellationToken cancellationToken = default)
    {
        DropOne(name, cancellationToken: cancellationToken);
        return Task.CompletedTask;
    }

    public Task DropOneAsync(string name, DropIndexOptions? options, CancellationToken cancellationToken = default)
    {
        DropOne(name, options, cancellationToken);
        return Task.CompletedTask;
    }

    public Task DropOneAsync(IClientSessionHandle session, string name, DropIndexOptions? options = null, CancellationToken cancellationToken = default)
    {
        DropOne(name, options, cancellationToken);
        return Task.CompletedTask;
    }

    public Task DropOneAsync(IClientSessionHandle session, string name, CancellationToken cancellationToken = default)
    {
        DropOne(name, cancellationToken: cancellationToken);
        return Task.CompletedTask;
    }

    public IAsyncCursor<BsonDocument> List(CancellationToken cancellationToken = default)
        => List(options: null, cancellationToken);

    public IAsyncCursor<BsonDocument> List(ListIndexesOptions? options, CancellationToken cancellationToken = default)
    {
        lock (_lock) { return new InMemoryAsyncCursor<BsonDocument>(_indexes.ToList()); }
    }

    public IAsyncCursor<BsonDocument> List(IClientSessionHandle session, CancellationToken cancellationToken = default)
        => List(cancellationToken);

    public IAsyncCursor<BsonDocument> List(IClientSessionHandle session, ListIndexesOptions? options, CancellationToken cancellationToken = default)
        => List(options, cancellationToken);

    public Task<IAsyncCursor<BsonDocument>> ListAsync(CancellationToken cancellationToken = default)
        => Task.FromResult(List(cancellationToken));

    public Task<IAsyncCursor<BsonDocument>> ListAsync(ListIndexesOptions? options, CancellationToken cancellationToken = default)
        => Task.FromResult(List(options, cancellationToken));

    public Task<IAsyncCursor<BsonDocument>> ListAsync(IClientSessionHandle session, CancellationToken cancellationToken = default)
        => Task.FromResult(List(cancellationToken));

    public Task<IAsyncCursor<BsonDocument>> ListAsync(IClientSessionHandle session, ListIndexesOptions? options, CancellationToken cancellationToken = default)
        => Task.FromResult(List(options, cancellationToken));

    /// <summary>
    /// Validates that a document does not violate any unique index constraints.
    /// Called before or after every write operation.
    /// </summary>
    /// <param name="doc">The document being inserted or the new state after update/replace.</param>
    /// <param name="excludeId">The _id of the document being updated/replaced (to exclude self from comparison).</param>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/core/index-unique/
    ///   "A unique index ensures that the indexed fields do not store duplicate values."
    /// </remarks>
    internal void ValidateDocument(BsonDocument doc, BsonValue? excludeId = null)
    {
        List<BsonDocument> uniqueIndexes;
        lock (_lock)
        {
            uniqueIndexes = _indexes.Where(idx => idx.Contains("unique") && idx["unique"].AsBoolean).ToList();
        }

        foreach (var indexDoc in uniqueIndexes)
        {
            var keyDoc = indexDoc["key"].AsBsonDocument;
            bool isSparse = indexDoc.Contains("sparse") && indexDoc["sparse"].AsBoolean;
            BsonDocument? partialFilter = indexDoc.Contains("partialFilterExpression")
                ? indexDoc["partialFilterExpression"].AsBsonDocument
                : null;

            var keyFields = keyDoc.Elements.Select(e => e.Name).ToList();

            // Ref: https://www.mongodb.com/docs/manual/core/index-sparse/#sparse-and-unique-properties
            //   "An index that is both sparse and unique prevents a collection from having documents
            //    with duplicate values for a field but allows multiple documents that omit the key."
            if (isSparse && keyFields.All(f => !FieldExistsInDoc(doc, f)))
                continue;

            // Ref: https://www.mongodb.com/docs/manual/core/index-partial/#partial-index-with-unique-constraint
            //   "The unique constraint only applies to the documents that meet the filter expression."
            if (partialFilter != null && !BsonFilterEvaluator.Matches(doc, partialFilter))
                continue;

            var newKeyValues = keyFields.Select(f => BsonFilterEvaluator.ResolveFieldPath(doc, f)).ToList();

            foreach (var existing in _store.GetAll())
            {
                if (excludeId != null && existing.Contains("_id") && existing["_id"].Equals(excludeId))
                    continue;

                // Sparse: skip existing docs that don't have the indexed fields
                if (isSparse && keyFields.All(f => !FieldExistsInDoc(existing, f)))
                    continue;

                // Partial: skip existing docs that don't match the filter
                if (partialFilter != null && !BsonFilterEvaluator.Matches(existing, partialFilter))
                    continue;

                var existingKeyValues = keyFields.Select(f => BsonFilterEvaluator.ResolveFieldPath(existing, f)).ToList();

                if (KeyValuesEqual(newKeyValues, existingKeyValues))
                {
                    var keyDesc = string.Join(", ", keyFields.Zip(newKeyValues, (f, v) => $"{f}: {v}"));
                    throw MongoErrors.DuplicateKeyIndex(indexDoc["name"].AsString, keyDesc);
                }
            }
        }
    }

    /// <summary>
    /// Checks if a document has expired according to any TTL index.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/core/index-ttl/#expiration-of-data
    ///   "TTL indexes expire documents after the specified number of seconds has passed
    ///    since the indexed field value."
    ///   "If the indexed field in a document doesn't contain one or more date values,
    ///    the document will not expire."
    ///   "If a document does not contain the indexed field, the document will not expire."
    /// </remarks>
    internal bool IsExpiredByTtl(BsonDocument doc)
    {
        List<BsonDocument> ttlIndexes;
        lock (_lock)
        {
            ttlIndexes = _indexes.Where(idx => idx.Contains("expireAfterSeconds")).ToList();
        }

        var now = DateTime.UtcNow;

        foreach (var indexDoc in ttlIndexes)
        {
            var ttlSeconds = indexDoc["expireAfterSeconds"].ToInt32();
            var keyDoc = indexDoc["key"].AsBsonDocument;
            var fieldName = keyDoc.Elements.First().Name;

            var fieldValue = BsonFilterEvaluator.ResolveFieldPath(doc, fieldName);

            DateTime? expirationBase = GetEarliestDate(fieldValue);
            if (expirationBase == null)
                continue;

            var expiresAt = expirationBase.Value.AddSeconds(ttlSeconds);
            if (now >= expiresAt)
                return true;
        }

        return false;
    }

    /// <summary>
    /// Returns the earliest date from a BsonValue (scalar date or array of dates).
    /// Returns null if the value is not a date or contains no dates.
    /// </summary>
    private static DateTime? GetEarliestDate(BsonValue value)
    {
        if (value.BsonType == BsonType.DateTime)
            return value.ToUniversalTime();

        // Ref: https://www.mongodb.com/docs/manual/core/index-ttl/#expiration-of-data
        //   "If the field is an array, and there are multiple date values in the index,
        //    MongoDB uses lowest (earliest) date value in the array to calculate the expiration threshold."
        if (value.BsonType == BsonType.Array)
        {
            DateTime? earliest = null;
            foreach (var element in value.AsBsonArray)
            {
                if (element.BsonType == BsonType.DateTime)
                {
                    var dt = element.ToUniversalTime();
                    if (earliest == null || dt < earliest.Value)
                        earliest = dt;
                }
            }
            return earliest;
        }

        return null;
    }

    private static bool FieldExistsInDoc(BsonDocument doc, string path)
    {
        var parts = path.Split('.');
        BsonValue current = doc;

        foreach (var part in parts)
        {
            if (current is BsonDocument nested)
            {
                if (!nested.Contains(part))
                    return false;
                current = nested[part];
            }
            else
            {
                return false;
            }
        }
        return true;
    }

    private static bool KeyValuesEqual(List<BsonValue> a, List<BsonValue> b)
    {
        if (a.Count != b.Count) return false;
        for (int i = 0; i < a.Count; i++)
        {
            if (!a[i].Equals(b[i]))
                return false;
        }
        return true;
    }

    /// <summary>
    /// Validates that existing documents in the store don't violate a unique constraint
    /// for a new index. Called during CreateOne.
    /// </summary>
    private void ValidateExistingDocuments(BsonDocument indexDoc)
    {
        var keyDoc = indexDoc["key"].AsBsonDocument;
        bool isSparse = indexDoc.Contains("sparse") && indexDoc["sparse"].AsBoolean;
        BsonDocument? partialFilter = indexDoc.Contains("partialFilterExpression")
            ? indexDoc["partialFilterExpression"].AsBsonDocument
            : null;

        var keyFields = keyDoc.Elements.Select(e => e.Name).ToList();
        var allDocs = _store.GetAll();

        var seen = new List<List<BsonValue>>();
        foreach (var doc in allDocs)
        {
            if (isSparse && keyFields.All(f => !FieldExistsInDoc(doc, f)))
                continue;
            if (partialFilter != null && !BsonFilterEvaluator.Matches(doc, partialFilter))
                continue;

            var values = keyFields.Select(f => BsonFilterEvaluator.ResolveFieldPath(doc, f)).ToList();

            if (seen.Any(s => KeyValuesEqual(s, values)))
            {
                var keyDesc = string.Join(", ", keyFields.Zip(values, (f, v) => $"{f}: {v}"));
                throw new MongoCommandException(
                    MongoErrors.SyntheticConnectionId,
                    $"Index build failed: {indexDoc["name"].AsString} dup key: {{ {keyDesc} }}",
                    new BsonDocument { { "ok", 0 }, { "code", 11000 } });
            }

            seen.Add(values);
        }
    }

    private static string GetOrGenerateIndexName(CreateIndexModel<TDocument> model)
    {
        if (!string.IsNullOrEmpty(model.Options?.Name))
            return model.Options.Name;

        var rendered = model.Keys.Render(new RenderArgs<TDocument>(
            BsonSerializer.LookupSerializer<TDocument>(),
            BsonSerializer.SerializerRegistry));

        return string.Join("_", rendered.Elements.Select(e => $"{e.Name}_{e.Value}"));
    }
}
