using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoDB.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for fault injection, operation logging, and concurrency stress.
/// </summary>
public class FaultInjectionAndLoggingTests
{
    #region Fault Injection

    [Fact]
    public void FaultInjector_ThrowsOnFind()
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("test");
        var col = (InMemoryMongoCollection<BsonDocument>)db.GetCollection<BsonDocument>("items");
        col.InsertOne(new BsonDocument("name", "test"));

        col.FaultInjector = (operation, _) =>
        {
            if (operation == "find")
                throw new InvalidOperationException("simulated find fault");
        };

        Assert.Throws<InvalidOperationException>(() =>
            col.Find(FilterDefinition<BsonDocument>.Empty).ToList());
    }

    [Fact]
    public void FaultInjector_ThrowsOnInsert()
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("test");
        var col = (InMemoryMongoCollection<BsonDocument>)db.GetCollection<BsonDocument>("items");

        col.FaultInjector = (operation, _) =>
        {
            if (operation == "insert")
                throw new InvalidOperationException("simulated insert fault");
        };

        Assert.Throws<InvalidOperationException>(() =>
            col.InsertOne(new BsonDocument("name", "test")));
    }

    [Fact]
    public void FaultInjector_ThrowsOnUpdate()
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("test");
        var col = (InMemoryMongoCollection<BsonDocument>)db.GetCollection<BsonDocument>("items");
        col.InsertOne(new BsonDocument("name", "test"));

        col.FaultInjector = (operation, _) =>
        {
            if (operation == "update")
                throw new InvalidOperationException("simulated update fault");
        };

        Assert.Throws<InvalidOperationException>(() =>
            col.UpdateOne(
                Builders<BsonDocument>.Filter.Eq("name", "test"),
                Builders<BsonDocument>.Update.Set("name", "changed")));
    }

    [Fact]
    public void FaultInjector_ThrowsOnDelete()
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("test");
        var col = (InMemoryMongoCollection<BsonDocument>)db.GetCollection<BsonDocument>("items");
        col.InsertOne(new BsonDocument("name", "test"));

        col.FaultInjector = (operation, _) =>
        {
            if (operation == "delete")
                throw new InvalidOperationException("simulated delete fault");
        };

        Assert.Throws<InvalidOperationException>(() =>
            col.DeleteOne(Builders<BsonDocument>.Filter.Eq("name", "test")));
    }

    [Fact]
    public void FaultInjector_ConditionalFaultBasedOnFilter()
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("test");
        var col = (InMemoryMongoCollection<BsonDocument>)db.GetCollection<BsonDocument>("items");
        col.InsertOne(new BsonDocument("name", "safe"));
        col.InsertOne(new BsonDocument("name", "dangerous"));

        col.FaultInjector = (operation, filter) =>
        {
            if (operation == "find" && filter != null && filter.Contains("name") && filter["name"] == "dangerous")
                throw new InvalidOperationException("fault on dangerous query");
        };

        // Safe query should work
        var results = col.Find(Builders<BsonDocument>.Filter.Eq("name", "safe")).ToList();
        Assert.Single(results);

        // Dangerous query should throw
        Assert.Throws<InvalidOperationException>(() =>
            col.Find(Builders<BsonDocument>.Filter.Eq("name", "dangerous")).ToList());
    }

    #endregion

    #region Operation Logging

    [Fact]
    public void OperationLog_RecordsInsertOperations()
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("test");
        var col = (InMemoryMongoCollection<BsonDocument>)db.GetCollection<BsonDocument>("items");

        col.InsertOne(new BsonDocument("name", "Alice"));
        col.InsertOne(new BsonDocument("name", "Bob"));

        var logs = col.OperationLog.GetAll();
        Assert.Equal(2, logs.Count);
        Assert.All(logs, l => Assert.Equal("InsertOne", l.Type));
    }

    [Fact]
    public void OperationLog_RecordsFindOperations()
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("test");
        var col = (InMemoryMongoCollection<BsonDocument>)db.GetCollection<BsonDocument>("items");
        col.InsertOne(new BsonDocument("status", "active"));

        col.Find(Builders<BsonDocument>.Filter.Eq("status", "active")).ToList();

        var findLogs = col.OperationLog.GetByType("Find");
        Assert.Single(findLogs);
        Assert.NotNull(findLogs[0].Filter);
        Assert.True(findLogs[0].Filter!.Contains("status"));
    }

    [Fact]
    public void OperationLog_RecordsUpdateWithFilterAndUpdate()
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("test");
        var col = (InMemoryMongoCollection<BsonDocument>)db.GetCollection<BsonDocument>("items");
        col.InsertOne(new BsonDocument("status", "pending"));

        col.UpdateOne(
            Builders<BsonDocument>.Filter.Eq("status", "pending"),
            Builders<BsonDocument>.Update.Set("status", "complete"));

        var updateLogs = col.OperationLog.GetByType("UpdateOne");
        Assert.Single(updateLogs);
        Assert.NotNull(updateLogs[0].Filter);
        Assert.True(updateLogs[0].Filter!.Contains("status"));
        Assert.NotNull(updateLogs[0].Update);
    }

    [Fact]
    public void OperationLog_RecordsDeleteOperations()
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("test");
        var col = (InMemoryMongoCollection<BsonDocument>)db.GetCollection<BsonDocument>("items");
        col.InsertOne(new BsonDocument("name", "test"));

        col.DeleteOne(Builders<BsonDocument>.Filter.Eq("name", "test"));

        var deleteLogs = col.OperationLog.GetByType("DeleteOne");
        Assert.Single(deleteLogs);
        Assert.NotNull(deleteLogs[0].Filter);
    }

    [Fact]
    public void OperationLog_RecordsAggregateOperations()
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("test");
        var col = (InMemoryMongoCollection<BsonDocument>)db.GetCollection<BsonDocument>("items");
        col.InsertOne(new BsonDocument { { "category", "A" }, { "value", 10 } });

        col.Aggregate<BsonDocument>(new[]
        {
            new BsonDocument("$group", new BsonDocument
            {
                { "_id", "$category" },
                { "total", new BsonDocument("$sum", "$value") }
            })
        }).ToList();

        var aggLogs = col.OperationLog.GetByType("Aggregate");
        Assert.Single(aggLogs);
        Assert.NotNull(aggLogs[0].Pipeline);
        Assert.Single(aggLogs[0].Pipeline!);
    }

    [Fact]
    public void OperationLog_CanBeCleared()
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("test");
        var col = (InMemoryMongoCollection<BsonDocument>)db.GetCollection<BsonDocument>("items");

        col.InsertOne(new BsonDocument("name", "test"));
        Assert.Equal(1, col.OperationLog.Count);

        col.OperationLog.Clear();
        Assert.Equal(0, col.OperationLog.Count);
    }

    #endregion

    #region Concurrency Stress Tests

    [Fact]
    public async Task ParallelInserts_DoNotCorruptState()
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("test");
        var col = db.GetCollection<BsonDocument>("items");

        const int threadCount = 10;
        const int insertsPerThread = 100;

        var tasks = Enumerable.Range(0, threadCount).Select(t =>
            Task.Run(() =>
            {
                for (int i = 0; i < insertsPerThread; i++)
                {
                    col.InsertOne(new BsonDocument
                    {
                        { "thread", t },
                        { "index", i }
                    });
                }
            })).ToArray();

        await Task.WhenAll(tasks);

        var count = col.CountDocuments(FilterDefinition<BsonDocument>.Empty);
        Assert.Equal(threadCount * insertsPerThread, count);
    }

    [Fact]
    public async Task ParallelUpdates_ProduceCorrectResults()
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("test");
        var col = db.GetCollection<BsonDocument>("items");

        // Create a document with a counter
        col.InsertOne(new BsonDocument { { "name", "counter" }, { "value", 0 } });

        const int updateCount = 100;
        var tasks = Enumerable.Range(0, updateCount).Select(_ =>
            Task.Run(() =>
            {
                col.UpdateOne(
                    Builders<BsonDocument>.Filter.Eq("name", "counter"),
                    Builders<BsonDocument>.Update.Inc("value", 1));
            })).ToArray();

        await Task.WhenAll(tasks);

        var doc = col.Find(Builders<BsonDocument>.Filter.Eq("name", "counter")).First();
        Assert.Equal(updateCount, doc["value"].AsInt32);
    }

    [Fact]
    public async Task ReadDuringWrite_ReturnsConsistentData()
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("test");
        var col = db.GetCollection<BsonDocument>("items");

        // Pre-populate
        for (int i = 0; i < 100; i++)
        {
            col.InsertOne(new BsonDocument("n", i));
        }

        var readErrors = 0;
        var writeComplete = false;

        // Writer thread
        var writer = Task.Run(() =>
        {
            for (int i = 100; i < 200; i++)
            {
                col.InsertOne(new BsonDocument("n", i));
            }
            Volatile.Write(ref writeComplete, true);
        });

        // Reader thread
        var reader = Task.Run(() =>
        {
            while (!Volatile.Read(ref writeComplete))
            {
                try
                {
                    var docs = col.Find(FilterDefinition<BsonDocument>.Empty).ToList();
                    // Should always get a consistent count (no partial states)
                    if (docs.Count < 100)
                    {
                        Interlocked.Increment(ref readErrors);
                    }
                }
                catch
                {
                    Interlocked.Increment(ref readErrors);
                }
            }
        });

        await Task.WhenAll(writer, reader);

        Assert.Equal(0, readErrors);
        Assert.Equal(200, col.CountDocuments(FilterDefinition<BsonDocument>.Empty));
    }

    #endregion
}
