using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;
using AwesomeAssertions;

namespace MongoDB.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Phase 4 integration tests: State persistence (export/import) and document clearing.
/// </summary>
public class StatePersistenceTests : IDisposable
{
    private readonly InMemoryMongoClient _client;
    private readonly InMemoryMongoDatabase _db;
    private string? _tempFile;

    public StatePersistenceTests()
    {
        _client = new InMemoryMongoClient();
        _db = (InMemoryMongoDatabase)_client.GetDatabase("persist_test");
    }

    public void Dispose()
    {
        if (_tempFile != null && File.Exists(_tempFile))
            File.Delete(_tempFile);
    }

    [Fact]
    public async Task ExportState_and_ImportState_roundtrip()
    {
        var collection = _db.GetCollection<BsonDocument>("docs");
        await collection.InsertManyAsync(new[]
        {
            new BsonDocument { { "_id", "a1" }, { "name", "Alice" } },
            new BsonDocument { { "_id", "a2" }, { "name", "Bob" } },
        });

        // Export
        var json = _db.ExportState();
        Assert.Contains("Alice", json);
        Assert.Contains("Bob", json);

        // Create a fresh database and import
        var client2 = new InMemoryMongoClient();
        var db2 = (InMemoryMongoDatabase)client2.GetDatabase("import_test");

        db2.ImportState(json);

        var imported = await db2.GetCollection<BsonDocument>("docs")
            .Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();

        Assert.Equal(2, imported.Count);
    }

    [Fact]
    public async Task ExportStateToFile_and_ImportStateFromFile()
    {
        _tempFile = Path.GetTempFileName();

        var collection = _db.GetCollection<BsonDocument>("file_persist");
        await collection.InsertOneAsync(new BsonDocument { { "_id", "f1" }, { "value", 42 } });

        _db.ExportStateToFile(_tempFile);

        Assert.True(File.Exists(_tempFile));
        var content = File.ReadAllText(_tempFile);
        Assert.Contains("42", content);

        // Import into fresh db
        var db2 = (InMemoryMongoDatabase)new InMemoryMongoClient().GetDatabase("fresh");
        db2.ImportStateFromFile(_tempFile);

        var result = await db2.GetCollection<BsonDocument>("file_persist")
            .Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();
        Assert.Single(result);
        Assert.Equal(42, result[0]["value"].AsInt32);
    }

    [Fact]
    public async Task ClearDocuments_empties_all_collections()
    {
        var coll1 = _db.GetCollection<BsonDocument>("clear1");
        var coll2 = _db.GetCollection<BsonDocument>("clear2");

        await coll1.InsertOneAsync(new BsonDocument { { "x", 1 } });
        await coll2.InsertOneAsync(new BsonDocument { { "x", 2 } });

        _db.ClearDocuments();

        var count1 = await coll1.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);
        var count2 = await coll2.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);

        Assert.Equal(0, count1);
        Assert.Equal(0, count2);
    }

    [Fact]
    public async Task ImportState_replaces_existing_data()
    {
        var collection = _db.GetCollection<BsonDocument>("replace_test");
        await collection.InsertOneAsync(new BsonDocument { { "_id", "old" }, { "data", "original" } });

        var newState = "{ \"replace_test\": [{ \"_id\": \"new1\", \"data\": \"imported\" }] }";
        _db.ImportState(newState);

        var results = await collection.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();
        Assert.Single(results);
        Assert.Equal("imported", results[0]["data"].AsString);
    }

    [Fact]
    public async Task ExportState_handles_multiple_collections()
    {
        var coll1 = _db.GetCollection<BsonDocument>("multi1");
        var coll2 = _db.GetCollection<BsonDocument>("multi2");

        await coll1.InsertOneAsync(new BsonDocument { { "key", "from_coll1" } });
        await coll2.InsertOneAsync(new BsonDocument { { "key", "from_coll2" } });

        var json = _db.ExportState();
        Assert.Contains("from_coll1", json);
        Assert.Contains("from_coll2", json);
    }
}
