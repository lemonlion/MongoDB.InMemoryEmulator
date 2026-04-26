using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace MongoDB.InMemoryEmulator;

/// <summary>
/// In-memory implementation of <see cref="IChangeStreamCursor{TDocument}"/>.
/// Receives change events from the <see cref="ChangeStreamNotifier"/> and returns them via cursor iteration.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/changeStreams/
///   "Change streams allow applications to access real-time data changes without the complexity
///    and risk of tailing the oplog."
/// </remarks>
internal class InMemoryChangeStreamCursor<TResult> : IChangeStreamCursor<TResult>
{
    private readonly ChangeStreamNotifier _notifier;
    private readonly string? _collectionFilter; // null = watch all collections
    private readonly string? _databaseFilter; // null = watch all databases
    private readonly ChangeStreamOptions? _options;
    private readonly IBsonSerializer<TResult> _outputSerializer;
    private long _lastSeenSequence;
    private IEnumerable<TResult> _current = Enumerable.Empty<TResult>();

    internal InMemoryChangeStreamCursor(
        ChangeStreamNotifier notifier,
        string? databaseFilter,
        string? collectionFilter,
        ChangeStreamOptions? options,
        IBsonSerializer<TResult> outputSerializer,
        long startSequence = 0)
    {
        _notifier = notifier;
        _databaseFilter = databaseFilter;
        _collectionFilter = collectionFilter;
        _options = options;
        _outputSerializer = outputSerializer;
        _lastSeenSequence = startSequence;

        // Handle StartAtOperationTime / ResumeAfter / StartAfter
        if (options?.ResumeAfter != null && options.ResumeAfter.Contains("_seq"))
            _lastSeenSequence = options.ResumeAfter["_seq"].ToInt64();
        else if (options?.StartAfter != null && options.StartAfter.Contains("_seq"))
            _lastSeenSequence = options.StartAfter["_seq"].ToInt64();
        else if (options?.StartAtOperationTime != null)
            _lastSeenSequence = options.StartAtOperationTime.Timestamp;
    }

    public IEnumerable<TResult> Current => _current;

    // Ref: https://www.mongodb.com/docs/manual/changeStreams/#resume-a-change-stream
    //   "Each change stream event document includes a resume token."
    public BsonDocument GetResumeToken()
    {
        return new BsonDocument("_seq", _lastSeenSequence);
    }

    public bool MoveNext(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var events = _notifier.GetEvents(_lastSeenSequence, _databaseFilter, _collectionFilter);
        if (events.Count == 0)
        {
            _current = Enumerable.Empty<TResult>();
            return false;
        }

        _lastSeenSequence = events[^1].Sequence;
        _current = events.Select(e => DeserializeEvent(e)).ToList();
        return true;
    }

    public async Task<bool> MoveNextAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Wait briefly for events (like real change stream polling)
        var events = _notifier.GetEvents(_lastSeenSequence, _databaseFilter, _collectionFilter);
        if (events.Count == 0)
        {
            // Async mode: wait for up to 100ms for new events
            await Task.Delay(50, cancellationToken);
            events = _notifier.GetEvents(_lastSeenSequence, _databaseFilter, _collectionFilter);
        }

        if (events.Count == 0)
        {
            _current = Enumerable.Empty<TResult>();
            return false;
        }

        _lastSeenSequence = events[^1].Sequence;
        _current = events.Select(e => DeserializeEvent(e)).ToList();
        return true;
    }

    private TResult DeserializeEvent(ChangeEvent evt)
    {
        var doc = evt.ToBsonDocument(_options);
        using var reader = new MongoDB.Bson.IO.BsonDocumentReader(doc);
        var ctx = BsonDeserializationContext.CreateRoot(reader);
        return _outputSerializer.Deserialize(ctx);
    }

    public void Dispose()
    {
        // No resources to release; cursor is purely in-memory.
    }
}

/// <summary>
/// Represents a single change event in the change stream.
/// </summary>
internal record ChangeEvent
{
    public long Sequence { get; init; }
    public ChangeStreamOperationType OperationType { get; init; }
    public string DatabaseName { get; init; } = "";
    public string CollectionName { get; init; } = "";
    public BsonDocument? DocumentKey { get; init; }
    public BsonDocument? FullDocument { get; init; }
    public BsonDocument? FullDocumentBeforeChange { get; init; }
    public BsonDocument? UpdateDescription { get; init; }
    public DateTime WallTime { get; init; } = DateTime.UtcNow;

    public BsonDocument ToBsonDocument(ChangeStreamOptions? options)
    {
        var doc = new BsonDocument
        {
            { "_id", new BsonDocument("_seq", Sequence) },
            { "operationType", OperationType.ToString().ToLowerInvariant() },
            { "ns", new BsonDocument
                {
                    { "db", DatabaseName },
                    { "coll", CollectionName }
                }
            },
            { "wallTime", new BsonDateTime(WallTime) },
            { "clusterTime", new BsonTimestamp((int)(Sequence & 0x7FFFFFFF), 1) },
        };

        if (DocumentKey != null)
            doc["documentKey"] = DocumentKey;

        // Ref: https://www.mongodb.com/docs/manual/changeStreams/#modify-change-stream-output
        //   "fullDocument: 'updateLookup' returns the most current majority-committed version."
        if (FullDocument != null
            && (OperationType == ChangeStreamOperationType.Insert
                || OperationType == ChangeStreamOperationType.Replace
                || options?.FullDocument == ChangeStreamFullDocumentOption.UpdateLookup
                || options?.FullDocument == ChangeStreamFullDocumentOption.WhenAvailable
                || options?.FullDocument == ChangeStreamFullDocumentOption.Required))
        {
            doc["fullDocument"] = FullDocument;
        }

        if (FullDocumentBeforeChange != null
            && (options?.FullDocumentBeforeChange == ChangeStreamFullDocumentBeforeChangeOption.WhenAvailable
                || options?.FullDocumentBeforeChange == ChangeStreamFullDocumentBeforeChangeOption.Required))
        {
            doc["fullDocumentBeforeChange"] = FullDocumentBeforeChange;
        }

        if (UpdateDescription != null && OperationType == ChangeStreamOperationType.Update)
            doc["updateDescription"] = UpdateDescription;

        return doc;
    }
}

/// <summary>
/// Central event bus for change stream notifications. Stores events and allows cursors to poll for new ones.
/// </summary>
/// <remarks>
/// Thread-safe: events are appended atomically and retrieved by sequence number.
/// </remarks>
internal class ChangeStreamNotifier
{
    private readonly List<ChangeEvent> _events = new();
    private readonly object _lock = new();
    private long _sequence;

    /// <summary>Publish a change event.</summary>
    internal void Publish(ChangeEvent evt)
    {
        lock (_lock)
        {
            evt = evt with { Sequence = Interlocked.Increment(ref _sequence) };
            _events.Add(evt);
        }
    }

    /// <summary>Get events after a given sequence number, optionally filtered by database/collection.</summary>
    internal List<ChangeEvent> GetEvents(long afterSequence, string? databaseFilter, string? collectionFilter)
    {
        lock (_lock)
        {
            return _events
                .Where(e => e.Sequence > afterSequence)
                .Where(e => databaseFilter == null || e.DatabaseName == databaseFilter)
                .Where(e => collectionFilter == null || e.CollectionName == collectionFilter)
                .ToList();
        }
    }

    /// <summary>Clear all events (for test reset).</summary>
    internal void Clear()
    {
        lock (_lock)
        {
            _events.Clear();
            _sequence = 0;
        }
    }

    /// <summary>Returns the current sequence number (for initializing cursors at "now").</summary>
    internal long CurrentSequence
    {
        get { lock (_lock) return _sequence; }
    }
}
