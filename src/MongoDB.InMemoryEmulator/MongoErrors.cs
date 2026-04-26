using System.Net;
using System.Reflection;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Clusters;
using MongoDB.Driver.Core.Connections;
using MongoDB.Driver.Core.Servers;

namespace MongoDB.InMemoryEmulator;

/// <summary>
/// Factory methods for constructing MongoDB driver exceptions with correct error codes.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/reference/error-codes/
///   MongoDB uses specific numeric error codes for different failure conditions.
/// </remarks>
internal static class MongoErrors
{
    // Synthetic ConnectionId used for all exceptions — emulates a local mongod endpoint.
    internal static readonly ConnectionId SyntheticConnectionId =
        new(new ServerId(new ClusterId(), new IPEndPoint(IPAddress.Loopback, 27017)));

    // WriteError and BulkWriteError have internal constructors in MongoDB.Driver 2.30.0.
    // No public API exists to create instances. Reflection is the only viable approach.
    // If the internal constructor is renamed or removed in a future version, CreateWriteError
    // will return null and callers should handle gracefully.
    private static readonly ConstructorInfo? WriteErrorCtor =
        typeof(WriteError).GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance,
            null,
            [typeof(ServerErrorCategory), typeof(int), typeof(string), typeof(BsonDocument)],
            null);

    private static readonly ConstructorInfo? BulkWriteErrorCtor =
        typeof(BulkWriteError).GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance,
            null,
            [typeof(int), typeof(ServerErrorCategory), typeof(int), typeof(string), typeof(BsonDocument)],
            null);

    internal static WriteError CreateWriteError(ServerErrorCategory category, int code, string message, BsonDocument? details = null)
    {
        if (WriteErrorCtor == null)
            throw new InvalidOperationException(
                "Could not find WriteError internal constructor. MongoDB.Driver version may have changed.");

        return (WriteError)WriteErrorCtor.Invoke([category, code, message, details!]);
    }

    internal static BulkWriteError CreateBulkWriteError(int index, ServerErrorCategory category, int code, string message, BsonDocument? details = null)
    {
        if (BulkWriteErrorCtor == null)
            throw new InvalidOperationException(
                "Could not find BulkWriteError internal constructor. MongoDB.Driver version may have changed.");

        return (BulkWriteError)BulkWriteErrorCtor.Invoke([index, category, code, message, details!]);
    }

    /// <summary>
    /// E11000 duplicate key error.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/error-codes/
    ///   "E11000 duplicate key error collection: {ns} index: {indexName} dup key: {keyValue}"
    /// </remarks>
    internal static MongoWriteException DuplicateKey(BsonValue id)
    {
        var writeError = CreateWriteError(
            ServerErrorCategory.DuplicateKey,
            11000,
            $"E11000 duplicate key error dup key: {{ _id: {id} }}");

        return new MongoWriteException(SyntheticConnectionId, writeError, writeConcernError: null, innerException: null);
    }

    /// <summary>
    /// E11000 duplicate key error for a secondary (non-_id) unique index.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/error-codes/
    ///   "E11000 duplicate key error collection: {ns} index: {indexName} dup key: {keyValue}"
    /// </remarks>
    internal static MongoWriteException DuplicateKeyIndex(string indexName, string keyDescription)
    {
        var writeError = CreateWriteError(
            ServerErrorCategory.DuplicateKey,
            11000,
            $"E11000 duplicate key error index: {indexName} dup key: {{ {keyDescription} }}");

        return new MongoWriteException(SyntheticConnectionId, writeError, writeConcernError: null, innerException: null);
    }

    /// <summary>
    /// Error code 66 — ImmutableField.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/error-codes/
    ///   "Performing an update on the path '_id' would modify the immutable field '_id'"
    /// </remarks>
    internal static MongoCommandException ImmutableField(string path)
    {
        return new MongoCommandException(
            SyntheticConnectionId,
            $"Performing an update on the path '{path}' would modify the immutable field '{path}'",
            new BsonDocument { { "ok", 0 }, { "code", 66 }, { "codeName", "ImmutableField" } });
    }

    /// <summary>
    /// Error code 2 — BadValue. Used when a replacement document contains update operators.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/error-codes/
    ///   "the update operation document must not contain atomic operators"
    /// </remarks>
    internal static MongoCommandException BadValue(string message)
    {
        return new MongoCommandException(
            SyntheticConnectionId,
            message,
            new BsonDocument { { "ok", 0 }, { "code", 2 }, { "codeName", "BadValue" } });
    }

    /// <summary>
    /// Error code 9 — FailedToParse. Used when an update document contains no operators.
    /// </summary>
    internal static MongoCommandException FailedToParse(string message)
    {
        return new MongoCommandException(
            SyntheticConnectionId,
            message,
            new BsonDocument { { "ok", 0 }, { "code", 9 }, { "codeName", "FailedToParse" } });
    }

    /// <summary>
    /// Error code 48 — NamespaceExists.
    /// </summary>
    internal static MongoCommandException NamespaceExists(string ns)
    {
        return new MongoCommandException(
            SyntheticConnectionId,
            $"a collection '{ns}' already exists",
            new BsonDocument { { "ok", 0 }, { "code", 48 }, { "codeName", "NamespaceExists" } });
    }

    /// <summary>
    /// Error code 26 — NamespaceNotFound.
    /// </summary>
    internal static MongoCommandException NamespaceNotFound(string ns)
    {
        return new MongoCommandException(
            SyntheticConnectionId,
            $"ns not found: {ns}",
            new BsonDocument { { "ok", 0 }, { "code", 26 }, { "codeName", "NamespaceNotFound" } });
    }

    // BulkWriteUpsert has an internal constructor: (int index, BsonValue id)
    // Reflection is the only viable approach — no public API exists.
    private static readonly ConstructorInfo? BulkWriteUpsertCtor =
        typeof(BulkWriteUpsert).GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance,
            null,
            [typeof(int), typeof(BsonValue)],
            null);

    internal static BulkWriteUpsert CreateBulkWriteUpsert(int index, BsonValue id)
    {
        if (BulkWriteUpsertCtor == null)
            throw new InvalidOperationException(
                "Could not find BulkWriteUpsert internal constructor. MongoDB.Driver version may have changed.");

        return (BulkWriteUpsert)BulkWriteUpsertCtor.Invoke([index, id]);
    }

    // BulkWriteResult<T>.Acknowledged has an internal constructor.
    // Reflection is used because no public factory exists.
    internal static BulkWriteResult<TDocument> CreateBulkWriteResult<TDocument>(
        int requestCount,
        long insertedCount,
        long matchedCount,
        long modifiedCount,
        long deletedCount,
        IEnumerable<BulkWriteUpsert> upserts,
        IReadOnlyList<WriteModel<TDocument>> processedRequests)
    {
        var ackType = typeof(BulkWriteResult<TDocument>)
            .GetNestedType("Acknowledged", BindingFlags.Public | BindingFlags.NonPublic);

        if (ackType == null)
            throw new InvalidOperationException(
                "Could not find BulkWriteResult.Acknowledged nested type. MongoDB.Driver version may have changed.");

        // Nested type on a generic parent is itself an open generic — must close it
        if (ackType.ContainsGenericParameters)
            ackType = ackType.MakeGenericType(typeof(TDocument));

        var ctor = ackType.GetConstructors(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance).FirstOrDefault();
        if (ctor == null)
            throw new InvalidOperationException(
                "Could not find BulkWriteResult.Acknowledged constructor. MongoDB.Driver version may have changed.");

        return (BulkWriteResult<TDocument>)ctor.Invoke([
            requestCount,
            matchedCount,
            deletedCount,
            insertedCount,
            (long?)modifiedCount,
            processedRequests,
            upserts
        ]);
    }
}
