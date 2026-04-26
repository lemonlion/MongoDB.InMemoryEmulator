using System.Collections.Concurrent;
using MongoDB.Bson;

namespace MongoDB.InMemoryEmulator;

/// <summary>
/// Records a single operation performed on an InMemoryMongoCollection.
/// Useful for asserting in tests that specific operations were issued.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/reference/command/
///   Operation logging allows test assertions on the commands issued to a collection.
/// </remarks>
public record OperationRecord
{
    public required string Type { get; init; }
    public BsonDocument? Filter { get; init; }
    public BsonDocument? Update { get; init; }
    public BsonDocument? Document { get; init; }
    public BsonDocument[]? Pipeline { get; init; }
    public DateTimeOffset Timestamp { get; init; } = DateTimeOffset.UtcNow;
}

/// <summary>
/// Thread-safe collection of operation records.
/// </summary>
public class OperationLog
{
    private readonly ConcurrentBag<OperationRecord> _records = new();

    public int Count => _records.Count;

    internal void Record(OperationRecord record)
    {
        _records.Add(record);
    }

    /// <summary>
    /// Gets all recorded operations.
    /// </summary>
    public IReadOnlyList<OperationRecord> GetAll()
    {
        return _records.ToArray();
    }

    /// <summary>
    /// Gets operations of a specific type.
    /// </summary>
    public IReadOnlyList<OperationRecord> GetByType(string operationType)
    {
        return _records.Where(r => r.Type == operationType).ToArray();
    }

    /// <summary>
    /// Clears all recorded operations.
    /// </summary>
    public void Clear()
    {
        _records.Clear();
    }
}
