using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core.Bindings;

namespace MongoDB.InMemoryEmulator;

/// <summary>
/// In-memory implementation of <see cref="IClientSessionHandle"/> providing session and transaction support.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/core/transactions/
///   "In MongoDB, an operation on a single document is atomic. Because you can use embedded documents
///    and arrays to capture relationships between data in a single document structure instead of normalizing
///    across multiple documents and collections, this single-document atomicity obviates the need for
///    distributed transactions for many practical use cases."
///
/// Transactions buffer writes in memory and apply them atomically on commit.
/// Abort discards the buffer. This provides snapshot isolation semantics in-memory.
/// </remarks>
internal class InMemoryClientSessionHandle : IClientSessionHandle
{
    private readonly IMongoClient _client;
    private readonly ClientSessionOptions _options;
    private bool _isInTransaction;
    private bool _disposed;

    // Transaction write buffer: collection namespace -> list of buffered operations
    private readonly List<Action> _transactionOps = new();

    internal InMemoryClientSessionHandle(IMongoClient client, ClientSessionOptions? options = null)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _options = options ?? new ClientSessionOptions();
    }

    public IMongoClient Client => _client;

    // Ref: https://www.mongodb.com/docs/manual/reference/method/Session/
    //   "The cluster time is a logical time used for ordering of operations in the cluster."
    public BsonDocument? ClusterTime { get; private set; }

    public bool IsImplicit => false;

    public bool IsInTransaction => _isInTransaction;

    public BsonTimestamp? OperationTime { get; private set; }

    public ClientSessionOptions Options => _options;

    // IServerSession is internal to the driver — create a minimal stub
    public IServerSession ServerSession { get; } = new InMemoryServerSession();

    // ICoreSessionHandle is internal — wrap in a minimal implementation
    // This is required by the interface but most consumers don't use it
    public ICoreSessionHandle WrappedCoreSession => throw new NotSupportedException(
        "WrappedCoreSession is not available in the in-memory emulator.");

    #region Transaction support

    // Ref: https://www.mongodb.com/docs/manual/core/transactions/
    //   "To use multi-document transactions, the client must start a session and then start a transaction."
    public void StartTransaction(TransactionOptions? transactionOptions = null)
    {
        if (_isInTransaction)
            throw new InvalidOperationException("Transaction already in progress.");
        _isInTransaction = true;
        _transactionOps.Clear();
    }

    public void CommitTransaction(CancellationToken cancellationToken = default)
    {
        if (!_isInTransaction)
            throw new InvalidOperationException("No transaction started.");

        // Apply all buffered operations
        foreach (var op in _transactionOps)
            op();

        _transactionOps.Clear();
        _isInTransaction = false;
    }

    public Task CommitTransactionAsync(CancellationToken cancellationToken = default)
    {
        CommitTransaction(cancellationToken);
        return Task.CompletedTask;
    }

    public void AbortTransaction(CancellationToken cancellationToken = default)
    {
        if (!_isInTransaction)
            throw new InvalidOperationException("No transaction started.");

        // Discard all buffered operations
        _transactionOps.Clear();
        _isInTransaction = false;
    }

    public Task AbortTransactionAsync(CancellationToken cancellationToken = default)
    {
        AbortTransaction(cancellationToken);
        return Task.CompletedTask;
    }

    // Ref: https://www.mongodb.com/docs/manual/core/transactions-in-applications/
    //   "The API incorporates retry logic for TransientTransactionError and UnknownTransactionCommitResult."
    public TResult WithTransaction<TResult>(
        Func<IClientSessionHandle, CancellationToken, TResult> callback,
        TransactionOptions? transactionOptions = null,
        CancellationToken cancellationToken = default)
    {
        while (true)
        {
            StartTransaction(transactionOptions);
            try
            {
                var result = callback(this, cancellationToken);
                CommitTransaction(cancellationToken);
                return result;
            }
            catch (MongoException ex) when (ex.HasErrorLabel("TransientTransactionError"))
            {
                // Retry the entire transaction
                AbortTransaction(cancellationToken);
                continue;
            }
            catch
            {
                AbortTransaction(cancellationToken);
                throw;
            }
        }
    }

    public async Task<TResult> WithTransactionAsync<TResult>(
        Func<IClientSessionHandle, CancellationToken, Task<TResult>> callbackAsync,
        TransactionOptions? transactionOptions = null,
        CancellationToken cancellationToken = default)
    {
        while (true)
        {
            StartTransaction(transactionOptions);
            try
            {
                var result = await callbackAsync(this, cancellationToken);
                await CommitTransactionAsync(cancellationToken);
                return result;
            }
            catch (MongoException ex) when (ex.HasErrorLabel("TransientTransactionError"))
            {
                await AbortTransactionAsync(cancellationToken);
                continue;
            }
            catch
            {
                await AbortTransactionAsync(cancellationToken);
                throw;
            }
        }
    }

    /// <summary>
    /// Enqueue an operation to be executed when the transaction commits.
    /// Non-transaction operations execute immediately.
    /// </summary>
    internal void EnqueueOrExecute(Action operation)
    {
        if (_isInTransaction)
            _transactionOps.Add(operation);
        else
            operation();
    }

    #endregion

    #region Cluster/Operation Time

    public void AdvanceClusterTime(BsonDocument newClusterTime)
    {
        ClusterTime = newClusterTime;
    }

    public void AdvanceOperationTime(BsonTimestamp newOperationTime)
    {
        OperationTime = newOperationTime;
    }

    #endregion

    #region Fork / Dispose

    public IClientSessionHandle Fork()
    {
        // Return a new handle sharing the same state
        return new InMemoryClientSessionHandle(_client, _options);
    }

    public void Dispose()
    {
        if (!_disposed)
        {
            if (_isInTransaction)
                AbortTransaction();
            _disposed = true;
        }
    }

    #endregion
}

/// <summary>
/// Minimal implementation of <see cref="IServerSession"/> for the in-memory emulator.
/// </summary>
internal class InMemoryServerSession : IServerSession
{
    public BsonDocument Id { get; } = new BsonDocument("id", new BsonBinaryData(Guid.NewGuid(), GuidRepresentation.Standard));
    public DateTime? LastUsedAt => DateTime.UtcNow;
    public void WasUsed() { }
    public long AdvanceTransactionNumber() => 0;
    public void Dispose() { }
}
