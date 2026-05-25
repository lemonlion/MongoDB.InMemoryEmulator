using global::MongoDB.Bson;
using global::MongoDB.Driver;

namespace InMemoryEmulator.MongoDB;

/// <summary>
/// Builds MongoDB wire-protocol-compatible reply documents for CommandSucceededEvent.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/reference/command/
///   Each command returns a specific reply document shape. This helper constructs
///   those shapes so that monitoring subscribers (e.g. TestTrackingDiagrams) can
///   extract response metadata just as they would from a real MongoDB server.
/// </remarks>
internal static class ReplyHelper
{
    /// <summary>
    /// Builds a cursor-style reply for find and aggregate operations.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/command/find/#output
    ///   { "ok": 1, "cursor": { "firstBatch": [...], "id": 0, "ns": "db.coll" } }
    /// </remarks>
    public static BsonDocument CursorReply(List<BsonDocument> documents, CollectionNamespace collectionNamespace)
    {
        return new BsonDocument
        {
            { "ok", 1 },
            { "cursor", new BsonDocument
                {
                    { "firstBatch", new BsonArray(documents) },
                    { "id", 0L },
                    { "ns", collectionNamespace.FullName }
                }
            }
        };
    }

    /// <summary>
    /// Builds a reply for delete operations.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/command/delete/#output
    ///   { "ok": 1, "n": &lt;deletedCount&gt; }
    /// </remarks>
    public static BsonDocument DeleteReply(DeleteResult result)
    {
        return new BsonDocument
        {
            { "ok", 1 },
            { "n", (int)result.DeletedCount }
        };
    }

    /// <summary>
    /// Builds a reply for update operations.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/command/update/#output
    ///   { "ok": 1, "n": &lt;matchedCount&gt;, "nModified": &lt;modifiedCount&gt; }
    /// </remarks>
    public static BsonDocument UpdateReply(UpdateResult result)
    {
        var reply = new BsonDocument
        {
            { "ok", 1 },
            { "n", (int)result.MatchedCount },
            { "nModified", (int)result.ModifiedCount }
        };

        if (result.UpsertedId != null)
            reply["nUpserted"] = 1;

        return reply;
    }

    /// <summary>
    /// Builds a reply for replaceOne operations.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/command/update/#output
    ///   ReplaceOne uses the update command wire format.
    /// </remarks>
    public static BsonDocument ReplaceReply(ReplaceOneResult result)
    {
        var reply = new BsonDocument
        {
            { "ok", 1 },
            { "n", (int)result.MatchedCount },
            { "nModified", (int)result.ModifiedCount }
        };

        if (result.UpsertedId != null)
            reply["nUpserted"] = 1;

        return reply;
    }

    /// <summary>
    /// Builds a reply for findAndModify operations.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/command/findAndModify/#output
    ///   { "ok": 1, "lastErrorObject": { "n": 1, "updatedExisting": true }, "value": {...} }
    ///   We include "n" at top level for compatibility with tracking subscribers.
    /// </remarks>
    public static BsonDocument FindAndModifyReply(bool documentFound)
    {
        return new BsonDocument
        {
            { "ok", 1 },
            { "n", documentFound ? 1 : 0 }
        };
    }
}
