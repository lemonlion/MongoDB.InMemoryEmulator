using MongoDB.Driver;

namespace MongoDB.InMemoryEmulator;

/// <summary>
/// In-memory implementation of <see cref="IAsyncCursor{T}"/> that iterates over a pre-computed list of documents.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/drivers/csharp/current/
///   IAsyncCursor provides batch-based iteration over query results.
///   MoveNext/MoveNextAsync returns false when all batches are exhausted.
///   Current contains the current batch of documents.
/// </remarks>
public sealed class InMemoryAsyncCursor<T> : IAsyncCursor<T>
{
    private readonly IReadOnlyList<T> _items;
    private readonly int _batchSize;
    private int _position;
    private IEnumerable<T>? _current;
    private bool _disposed;

    public InMemoryAsyncCursor(IEnumerable<T> items, int batchSize = 101)
    {
        _items = items as IReadOnlyList<T> ?? items.ToList();
        _batchSize = batchSize > 0 ? batchSize : 101;
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
        cancellationToken.ThrowIfCancellationRequested();

        if (_position >= _items.Count)
        {
            _current = null;
            return false;
        }

        var batch = _items.Skip(_position).Take(_batchSize).ToList();
        _position += batch.Count;
        _current = batch;
        return true;
    }

    public Task<bool> MoveNextAsync(CancellationToken cancellationToken = default)
    {
        return Task.FromResult(MoveNext(cancellationToken));
    }

    public void Dispose()
    {
        _disposed = true;
    }
}
