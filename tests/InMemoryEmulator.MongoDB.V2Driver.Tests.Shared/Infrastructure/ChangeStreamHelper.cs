using global::MongoDB.Bson;
using global::MongoDB.Driver;

namespace InMemoryEmulator.MongoDB.Tests.Infrastructure;

/// <summary>
/// Helper for consuming change stream events in tests.
/// In-memory delivers events synchronously; real MongoDB requires polling with timeouts.
/// </summary>
public static class ChangeStreamHelper
{
    /// <summary>
    /// Polls the change stream cursor until the expected number of events are received or timeout is reached.
    /// Works with both in-memory (synchronous) and real MongoDB (async polling).
    /// </summary>
    public static async Task<List<BsonDocument>> WaitForEventsAsync(
        IChangeStreamCursor<BsonDocument> cursor,
        int expectedCount,
        TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(10));
        var events = new List<BsonDocument>();

        while (events.Count < expectedCount && DateTime.UtcNow < deadline)
        {
            if (await cursor.MoveNextAsync())
            {
                events.AddRange(cursor.Current);
            }
            else
            {
                await Task.Delay(50);
            }
        }

        return events;
    }

    /// <summary>
    /// Polls the change stream cursor until at least one event is received or timeout is reached.
    /// Returns the list of events from the first non-empty batch.
    /// </summary>
    public static Task<List<BsonDocument>> WaitForEventsAsync(
        IChangeStreamCursor<BsonDocument> cursor,
        TimeSpan? timeout = null)
        => WaitForEventsAsync(cursor, 1, timeout);
}
