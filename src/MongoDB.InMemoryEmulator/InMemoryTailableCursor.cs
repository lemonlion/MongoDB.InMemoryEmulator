using System.Threading.Channels;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace MongoDB.InMemoryEmulator;

/// <summary>
/// Tailable cursor for capped collections. Stays open after exhausting current results
/// and yields new documents as they are inserted.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/core/tailable-cursors/
///   "A tailable cursor remains open after the client exhausts the results in the initial cursor.
///    Clients can continue to iterate the tailable cursor as new data is inserted into the collection."
///
/// Ref: https://www.mongodb.com/docs/manual/reference/method/cursor.tailable/
///   "With a tailable cursor, after a client exhausts the results in the initial cursor,
///    the cursor may remain open and return additional results as more documents are added."
/// </remarks>
internal sealed class InMemoryTailableCursor<T> : IAsyncCursor<T>
{
    private readonly DocumentStore _store;
    private readonly BsonDocument _filter;
    private readonly IBsonSerializer<T> _serializer;
    private readonly bool _awaitData;
    private readonly TimeSpan _maxAwaitTime;
    private readonly Channel<BsonDocument> _channel;
    private IDisposable? _subscription;
    private IEnumerable<T>? _current;
    private bool _initialBatchConsumed;
    private bool _disposed;
    private int _nextIndex;

    public InMemoryTailableCursor(
        DocumentStore store,
        BsonDocument filter,
        IBsonSerializer<T> serializer,
        bool awaitData = false,
        TimeSpan? maxAwaitTime = null)
    {
        _store = store;
        _filter = filter;
        _serializer = serializer;
        _awaitData = awaitData;
        _maxAwaitTime = maxAwaitTime ?? TimeSpan.FromSeconds(1);
        _channel = Channel.CreateUnbounded<BsonDocument>();

        // Subscribe to inserts on the store
        _subscription = store.SubscribeToInserts(doc =>
        {
            if (BsonFilterEvaluator.Matches(doc, _filter))
            {
                _channel.Writer.TryWrite(doc.DeepClone().AsBsonDocument);
            }
        });
    }

    public IEnumerable<T> Current
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _current ?? throw new InvalidOperationException("MoveNext must be called before accessing Current.");
        }
    }

    public bool MoveNext(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (!_initialBatchConsumed)
        {
            // Return initial results from the capped collection in insertion order
            _initialBatchConsumed = true;
            var allDocs = _store.GetAll();
            var matched = allDocs.Where(d => BsonFilterEvaluator.Matches(d, _filter)).ToList();
            _nextIndex = allDocs.Count;

            var items = matched.Select(Deserialize).ToList();
            _current = items;
            return true;
        }

        // Try to read from the channel (new inserts)
        var batch = new List<T>();
        while (_channel.Reader.TryRead(out var doc))
        {
            batch.Add(Deserialize(doc));
        }

        if (batch.Count > 0)
        {
            _current = batch;
            return true;
        }

        if (_awaitData)
        {
            // Block until data or timeout
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(_maxAwaitTime);
            try
            {
                if (_channel.Reader.WaitToReadAsync(cts.Token).AsTask().GetAwaiter().GetResult())
                {
                    while (_channel.Reader.TryRead(out var doc2))
                    {
                        batch.Add(Deserialize(doc2));
                    }
                    _current = batch;
                    return true;
                }
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                // Timeout — return empty batch (tailable cursor stays open)
                _current = batch;
                return true;
            }
        }

        // Tailable cursor: return true with empty batch (cursor remains open)
        _current = batch;
        return true;
    }

    public async Task<bool> MoveNextAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (!_initialBatchConsumed)
        {
            _initialBatchConsumed = true;
            var allDocs = _store.GetAll();
            var matched = allDocs.Where(d => BsonFilterEvaluator.Matches(d, _filter)).ToList();
            _nextIndex = allDocs.Count;

            var items = matched.Select(Deserialize).ToList();
            _current = items;
            return true;
        }

        var batch = new List<T>();
        while (_channel.Reader.TryRead(out var doc))
        {
            batch.Add(Deserialize(doc));
        }

        if (batch.Count > 0)
        {
            _current = batch;
            return true;
        }

        if (_awaitData)
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(_maxAwaitTime);
            try
            {
                if (await _channel.Reader.WaitToReadAsync(cts.Token))
                {
                    while (_channel.Reader.TryRead(out var doc2))
                    {
                        batch.Add(Deserialize(doc2));
                    }
                    _current = batch;
                    return true;
                }
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                _current = batch;
                return true;
            }
        }

        _current = batch;
        return true;
    }

    private T Deserialize(BsonDocument doc)
    {
        if (typeof(T) == typeof(BsonDocument))
            return (T)(object)doc;
        return BsonSerializer.Deserialize<T>(doc);
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            _disposed = true;
            _subscription?.Dispose();
            _channel.Writer.TryComplete();
        }
    }
}
