using MongoDB.Bson;

namespace MongoDB.InMemoryEmulator;

/// <summary>
/// Provides context for aggregation pipeline execution,
/// allowing stages like $lookup, $merge, $out, and $unionWith to resolve
/// collections from the current or other databases.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/aggregation/
///   "An aggregation pipeline consists of one or more stages that process documents."
/// </remarks>
internal class AggregationContext
{
    private readonly InMemoryMongoDatabase _database;
    private readonly InMemoryMongoClient? _client;
    private readonly BsonDocument? _variables;

    internal AggregationContext(InMemoryMongoDatabase database, InMemoryMongoClient? client = null, BsonDocument? variables = null)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _client = client;
        _variables = variables;
    }

    /// <summary>The current pipeline variables (from $lookup let, etc.).</summary>
    internal BsonDocument? Variables => _variables;

    /// <summary>The namespace of the source collection for the pipeline.</summary>
    internal string? CollectionNamespace { get; init; }

    /// <summary>Document count in the source collection (for $collStats).</summary>
    internal long DocumentCount { get; init; }

    /// <summary>Get all documents from a collection in the current database.</summary>
    internal IEnumerable<BsonDocument> GetCollectionDocuments(string collectionName)
    {
        var store = _database.GetStore(collectionName);
        return store?.GetAll() ?? Enumerable.Empty<BsonDocument>();
    }

    /// <summary>Get a DocumentStore from the current database (for $merge/$out writes).</summary>
    internal DocumentStore GetOrCreateStore(string collectionName)
    {
        return _database.GetOrCreateStore(collectionName);
    }

    /// <summary>
    /// Get documents from a collection in another database (cross-database $lookup/$merge/$out).
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/lookup/
    ///   "Starting in MongoDB 5.1, $lookup can specify a collection in another database."
    /// </remarks>
    internal IEnumerable<BsonDocument> GetCollectionDocuments(string databaseName, string collectionName)
    {
        if (_client == null)
            throw new NotSupportedException("Cross-database $lookup requires an InMemoryMongoClient context.");

        var db = (InMemoryMongoDatabase)_client.GetDatabase(databaseName);
        var store = db.GetStore(collectionName);
        return store?.GetAll() ?? Enumerable.Empty<BsonDocument>();
    }

    /// <summary>Resolve a $$ variable reference.</summary>
    internal BsonValue? ResolveVariable(string name)
    {
        if (_variables != null && _variables.Contains(name))
            return _variables[name];
        // Built-in system variables
        return name switch
        {
            "NOW" => new BsonDateTime(DateTime.UtcNow),
            "CLUSTER_TIME" => new BsonTimestamp((int)DateTimeOffset.UtcNow.ToUnixTimeSeconds(), 1),
            "ROOT" => BsonNull.Value, // Set per-doc in pipeline context
            "CURRENT" => BsonNull.Value,
            "REMOVE" => BsonNull.Value, // Special: signal to remove field
            _ => null
        };
    }

    /// <summary>Create a new context with additional let variables.</summary>
    internal AggregationContext WithVariables(BsonDocument vars)
    {
        var merged = _variables != null ? _variables.DeepClone().AsBsonDocument : new BsonDocument();
        foreach (var element in vars)
            merged[element.Name] = element.Value;
        return new AggregationContext(_database, _client, merged)
        {
            CollectionNamespace = CollectionNamespace,
            DocumentCount = DocumentCount
        };
    }
}
