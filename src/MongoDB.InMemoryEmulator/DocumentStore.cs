using System.Collections.Concurrent;
using MongoDB.Bson;

namespace MongoDB.InMemoryEmulator;

/// <summary>
/// Represents a change event recorded in the document store's change log.
/// Used to power change streams.
/// </summary>
internal record ChangeEvent(
    long SequenceNumber,
    ChangeEventType OperationType,
    BsonValue DocumentId,
    BsonDocument? FullDocument,
    BsonDocument? FullDocumentBeforeChange,
    BsonDocument? UpdateDescription,
    DateTimeOffset Timestamp);

internal enum ChangeEventType
{
    Insert,
    Update,
    Replace,
    Delete
}

/// <summary>
/// Internal BSON document store backing an in-memory collection.
/// Thread-safe via ConcurrentDictionary and per-document locks.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/core/document/
///   Documents are stored as BSON, keyed by _id. The _id field is immutable after insertion.
///   Maximum document size is 16 megabytes.
/// </remarks>
internal class DocumentStore
{
    private const int MaxDocumentSizeBytes = 16 * 1024 * 1024;

    private readonly ConcurrentDictionary<BsonValue, BsonDocument> _documents = new(BsonValueComparer.Instance);
    private readonly ConcurrentDictionary<BsonValue, long> _versions = new(BsonValueComparer.Instance);
    private readonly List<ChangeEvent> _changeLog = new();
    private long _changeLogSequence;
    private readonly ConcurrentDictionary<BsonValue, SemaphoreSlim> _docLocks = new(BsonValueComparer.Instance);
    private readonly SemaphoreSlim _collectionLock = new(1, 1);
    private readonly object _changeLogLock = new();

    internal IReadOnlyList<ChangeEvent> ChangeLog
    {
        get
        {
            lock (_changeLogLock)
            {
                return _changeLog.ToList();
            }
        }
    }

    internal int Count => _documents.Count;

    /// <summary>
    /// Gets a snapshot of all documents.
    /// </summary>
    internal IReadOnlyList<BsonDocument> GetAll()
    {
        return _documents.Values.Select(d => d.DeepClone().AsBsonDocument).ToList();
    }

    /// <summary>
    /// Gets a document by _id, or null if not found.
    /// </summary>
    internal BsonDocument? Get(BsonValue id)
    {
        return _documents.TryGetValue(id, out var doc) ? doc.DeepClone().AsBsonDocument : null;
    }

    /// <summary>
    /// Inserts a document. Auto-generates _id if not present.
    /// Throws on duplicate _id (E11000).
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/command/insert/
    ///   "If the document does not specify an _id field, then mongod will add the _id field
    ///    and assign a unique ObjectId() for the document before inserting."
    /// </remarks>
    internal BsonDocument Insert(BsonDocument doc)
    {
        EnsureId(doc);
        ValidateDocumentSize(doc);

        var id = doc["_id"];

        if (!_documents.TryAdd(id, doc.DeepClone().AsBsonDocument))
        {
            // Ref: https://www.mongodb.com/docs/manual/reference/error-codes/
            //   E11000 duplicate key error
            throw MongoErrors.DuplicateKey(id);
        }

        _versions[id] = 1;
        RecordChange(ChangeEventType.Insert, id, doc.DeepClone().AsBsonDocument, null, null);
        return doc;
    }

    /// <summary>
    /// Replaces a document by _id. Returns true if a document was replaced.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/command/update/
    ///   "If the update parameter contains only field:value expressions, then update replaces
    ///    the matching document with the update document."
    /// </remarks>
    internal bool Replace(BsonValue id, BsonDocument replacement)
    {
        ValidateDocumentSize(replacement);

        if (!_documents.TryGetValue(id, out var existing))
            return false;

        var before = existing.DeepClone().AsBsonDocument;
        var newDoc = replacement.DeepClone().AsBsonDocument;

        // Ensure _id is preserved
        if (!newDoc.Contains("_id"))
            newDoc.InsertAt(0, new BsonElement("_id", id));

        _documents[id] = newDoc;
        _versions.AddOrUpdate(id, 1, (_, v) => v + 1);
        RecordChange(ChangeEventType.Replace, id, newDoc.DeepClone().AsBsonDocument, before, null);
        return true;
    }

    /// <summary>
    /// Updates a document in place. The updateAction receives a deep clone
    /// and should return the updated document.
    /// Returns (matched, modified, beforeDoc).
    /// </summary>
    internal (bool Matched, bool Modified, BsonDocument? Before) Update(BsonValue id, Func<BsonDocument, BsonDocument> updateAction)
    {
        if (!_documents.TryGetValue(id, out var existing))
            return (false, false, null);

        var before = existing.DeepClone().AsBsonDocument;
        var updated = updateAction(existing.DeepClone().AsBsonDocument);
        ValidateDocumentSize(updated);

        // Ensure _id is not changed
        if (!updated.Contains("_id"))
            updated.InsertAt(0, new BsonElement("_id", id));

        bool modified = !existing.Equals(updated);
        if (modified)
        {
            _documents[id] = updated;
            _versions.AddOrUpdate(id, 1, (_, v) => v + 1);

            var updateDesc = BuildUpdateDescription(before, updated);
            RecordChange(ChangeEventType.Update, id, updated.DeepClone().AsBsonDocument, before, updateDesc);
        }

        return (true, modified, before);
    }

    /// <summary>
    /// Removes a document by _id. Returns the removed document or null.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/command/delete/
    ///   "The delete command removes documents from a collection."
    /// </remarks>
    internal BsonDocument? Remove(BsonValue id)
    {
        if (!_documents.TryRemove(id, out var removed))
            return null;

        _versions.TryRemove(id, out _);
        _docLocks.TryRemove(id, out _);
        RecordChange(ChangeEventType.Delete, id, null, removed, null);
        return removed;
    }

    /// <summary>
    /// Clears all documents and the change log.
    /// </summary>
    internal void Clear()
    {
        _documents.Clear();
        _versions.Clear();
        _docLocks.Clear();
        lock (_changeLogLock)
        {
            _changeLog.Clear();
            _changeLogSequence = 0;
        }
    }

    /// <summary>
    /// Acquires the collection-level lock for multi-document operations.
    /// </summary>
    internal async Task<IDisposable> AcquireCollectionLockAsync(CancellationToken cancellationToken = default)
    {
        await _collectionLock.WaitAsync(cancellationToken);
        return new CollectionLockRelease(_collectionLock);
    }

    internal static void EnsureId(BsonDocument doc)
    {
        // Ref: https://www.mongodb.com/docs/manual/core/document/#the-_id-field
        //   "If an inserted document omits the _id field, the MongoDB driver automatically
        //    generates an ObjectId for the _id field."
        if (!doc.Contains("_id") || doc["_id"] == BsonNull.Value)
        {
            doc.Remove("_id");
            doc.InsertAt(0, new BsonElement("_id", ObjectId.GenerateNewId()));
        }
    }

    private static void ValidateDocumentSize(BsonDocument doc)
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/limits/#mongodb-limit-BSON-Document-Size
        //   "The maximum BSON document size is 16 megabytes."
        var size = doc.ToBson().Length;
        if (size > MaxDocumentSizeBytes)
        {
            throw new MongoDB.Driver.MongoCommandException(
                null!,
                $"Document exceeds maximum size of {MaxDocumentSizeBytes} bytes (actual: {size})",
                new BsonDocument("ok", 0));
        }
    }

    private void RecordChange(ChangeEventType type, BsonValue id, BsonDocument? fullDoc, BsonDocument? beforeDoc, BsonDocument? updateDesc)
    {
        lock (_changeLogLock)
        {
            var seq = Interlocked.Increment(ref _changeLogSequence);
            _changeLog.Add(new ChangeEvent(seq, type, id, fullDoc, beforeDoc, updateDesc, DateTimeOffset.UtcNow));
        }
    }

    private static BsonDocument BuildUpdateDescription(BsonDocument before, BsonDocument after)
    {
        var updatedFields = new BsonDocument();
        var removedFields = new BsonArray();

        foreach (var element in after)
        {
            if (element.Name == "_id") continue;
            if (!before.Contains(element.Name) || !before[element.Name].Equals(element.Value))
            {
                updatedFields[element.Name] = element.Value;
            }
        }

        foreach (var element in before)
        {
            if (element.Name == "_id") continue;
            if (!after.Contains(element.Name))
            {
                removedFields.Add(element.Name);
            }
        }

        var desc = new BsonDocument();
        if (updatedFields.ElementCount > 0) desc["updatedFields"] = updatedFields;
        if (removedFields.Count > 0) desc["removedFields"] = removedFields;
        return desc;
    }

    private sealed class CollectionLockRelease(SemaphoreSlim semaphore) : IDisposable
    {
        public void Dispose() => semaphore.Release();
    }
}
