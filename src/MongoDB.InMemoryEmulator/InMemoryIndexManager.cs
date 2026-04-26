using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace MongoDB.InMemoryEmulator;

/// <summary>
/// In-memory implementation of <see cref="IMongoIndexManager{TDocument}"/>.
/// Phase 1: accepts and stores index models, returns index names.
/// Phase 10 will add full enforcement (unique compound, sparse, partial filter, TTL).
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
