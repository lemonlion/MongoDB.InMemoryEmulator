using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoDB.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for capped collections, tailable cursors, $jsonSchema, and JavaScript expressions.
/// </summary>
public class CappedCollectionTests
{
    private InMemoryMongoClient CreateClient() => new();

    #region Capped Collection FIFO Eviction

    [Fact]
    public void CappedCollection_EvictsOldestWhenMaxDocumentsExceeded()
    {
        // Ref: https://www.mongodb.com/docs/manual/core/capped-collections/
        //   "Once a capped collection reaches its maximum size, inserts remove the oldest
        //    documents in the collection to make room for the new documents."
        var client = CreateClient();
        var db = client.GetDatabase("test");
        db.CreateCollection("capped", new CreateCollectionOptions
        {
            Capped = true,
            MaxDocuments = 3,
            MaxSize = 1_000_000
        });

        var col = db.GetCollection<BsonDocument>("capped");
        col.InsertOne(new BsonDocument("name", "first"));
        col.InsertOne(new BsonDocument("name", "second"));
        col.InsertOne(new BsonDocument("name", "third"));

        // Insert 4th — should evict "first"
        col.InsertOne(new BsonDocument("name", "fourth"));

        var docs = col.Find(FilterDefinition<BsonDocument>.Empty).ToList();
        Assert.Equal(3, docs.Count);
        Assert.DoesNotContain(docs, d => d["name"] == "first");
        Assert.Contains(docs, d => d["name"] == "second");
        Assert.Contains(docs, d => d["name"] == "third");
        Assert.Contains(docs, d => d["name"] == "fourth");
    }

    [Fact]
    public void CappedCollection_RespectsMaxDocuments()
    {
        var client = CreateClient();
        var db = client.GetDatabase("test");
        db.CreateCollection("capped", new CreateCollectionOptions
        {
            Capped = true,
            MaxDocuments = 5,
            MaxSize = 1_000_000
        });

        var col = db.GetCollection<BsonDocument>("capped");
        for (int i = 0; i < 10; i++)
        {
            col.InsertOne(new BsonDocument("n", i));
        }

        var docs = col.Find(FilterDefinition<BsonDocument>.Empty).ToList();
        Assert.Equal(5, docs.Count);
        // Should have docs 5-9 (the last 5)
        Assert.Equal(5, docs[0]["n"].AsInt32);
        Assert.Equal(9, docs[4]["n"].AsInt32);
    }

    [Fact]
    public void CappedCollection_PreservesInsertionOrder()
    {
        // Ref: https://www.mongodb.com/docs/manual/core/capped-collections/
        //   "The insertion order is preserved in capped collections."
        var client = CreateClient();
        var db = client.GetDatabase("test");
        db.CreateCollection("capped", new CreateCollectionOptions
        {
            Capped = true,
            MaxDocuments = 100,
            MaxSize = 1_000_000
        });

        var col = db.GetCollection<BsonDocument>("capped");
        for (int i = 0; i < 10; i++)
        {
            col.InsertOne(new BsonDocument("n", i));
        }

        var docs = col.Find(FilterDefinition<BsonDocument>.Empty).ToList();
        for (int i = 0; i < 10; i++)
        {
            Assert.Equal(i, docs[i]["n"].AsInt32);
        }
    }

    [Fact]
    public void CappedCollection_CannotDeleteIndividualDocuments()
    {
        // Ref: https://www.mongodb.com/docs/manual/core/capped-collections/
        //   "You cannot delete documents from a capped collection."
        var client = CreateClient();
        var db = client.GetDatabase("test");
        db.CreateCollection("capped", new CreateCollectionOptions
        {
            Capped = true,
            MaxDocuments = 10,
            MaxSize = 1_000_000
        });

        var col = db.GetCollection<BsonDocument>("capped");
        col.InsertOne(new BsonDocument("name", "test"));

        Assert.Throws<MongoCommandException>(() =>
            col.DeleteOne(Builders<BsonDocument>.Filter.Eq("name", "test")));
    }

    [Fact]
    public void CappedCollection_CanBeDropped()
    {
        var client = CreateClient();
        var db = client.GetDatabase("test");
        db.CreateCollection("capped", new CreateCollectionOptions
        {
            Capped = true,
            MaxDocuments = 10,
            MaxSize = 1_000_000
        });

        var col = db.GetCollection<BsonDocument>("capped");
        col.InsertOne(new BsonDocument("name", "test"));

        // Drop should work (not an individual delete)
        db.DropCollection("capped");
        var newCol = db.GetCollection<BsonDocument>("capped");
        Assert.Equal(0, newCol.CountDocuments(FilterDefinition<BsonDocument>.Empty));
    }

    [Fact]
    public void CappedCollection_RespectsMaxSize()
    {
        var client = CreateClient();
        var db = client.GetDatabase("test");
        // Very small MaxSize to trigger eviction
        db.CreateCollection("capped", new CreateCollectionOptions
        {
            Capped = true,
            MaxSize = 500 // Very small — will evict quickly
        });

        var col = db.GetCollection<BsonDocument>("capped");
        // Insert documents with enough data to exceed MaxSize
        for (int i = 0; i < 20; i++)
        {
            col.InsertOne(new BsonDocument
            {
                { "n", i },
                { "data", new string('x', 100) }
            });
        }

        var docs = col.Find(FilterDefinition<BsonDocument>.Empty).ToList();
        // Should have fewer than 20 docs due to MaxSize eviction
        Assert.True(docs.Count < 20, $"Expected fewer than 20 docs, got {docs.Count}");
        Assert.True(docs.Count > 0, "Should have at least one document");
    }

    #endregion

    #region Tailable Cursor

    [Fact]
    public async Task TailableCursor_ReceivesNewDocuments()
    {
        // Ref: https://www.mongodb.com/docs/manual/core/tailable-cursors/
        //   "A tailable cursor remains open after the client exhausts the results in the
        //    initial cursor."
        var client = CreateClient();
        var db = client.GetDatabase("test");
        db.CreateCollection("capped_tail", new CreateCollectionOptions
        {
            Capped = true,
            MaxDocuments = 100,
            MaxSize = 1_000_000
        });

        var col = db.GetCollection<BsonDocument>("capped_tail");
        col.InsertOne(new BsonDocument("n", 1));

        var options = new FindOptions<BsonDocument, BsonDocument>
        {
            CursorType = CursorType.TailableAwait,
            MaxAwaitTime = TimeSpan.FromMilliseconds(200)
        };

        using var cursor = col.FindSync(FilterDefinition<BsonDocument>.Empty, options);

        // First MoveNext returns initial batch
        Assert.True(await cursor.MoveNextAsync(), "First MoveNextAsync should return true");
        Assert.Single(cursor.Current);
        Assert.Equal(1, cursor.Current.First()["n"].AsInt32);

        // Insert a new doc while cursor is open
        col.InsertOne(new BsonDocument("n", 2));

        // Allow propagation
        await Task.Delay(50);

        // Next MoveNext should pick up the new doc
        Assert.True(await cursor.MoveNextAsync(), "Second MoveNextAsync should return true");
        var items = cursor.Current.ToList();
        Assert.Single(items);
        Assert.Equal(2, items[0]["n"].AsInt32);
    }

    [Fact]
    public async Task TailableCursor_ReturnsEmptyBatchOnTimeout()
    {
        var client = CreateClient();
        var db = client.GetDatabase("test");
        db.CreateCollection("capped_timeout", new CreateCollectionOptions
        {
            Capped = true,
            MaxDocuments = 100,
            MaxSize = 1_000_000
        });

        var col = db.GetCollection<BsonDocument>("capped_timeout");
        col.InsertOne(new BsonDocument("n", 1));

        var options = new FindOptions<BsonDocument, BsonDocument>
        {
            CursorType = CursorType.TailableAwait,
            MaxAwaitTime = TimeSpan.FromMilliseconds(100)
        };

        using var cursor = col.FindSync(FilterDefinition<BsonDocument>.Empty, options);

        // Consume initial batch
        Assert.True(await cursor.MoveNextAsync());

        // No new inserts — should return true with empty batch after timeout
        Assert.True(await cursor.MoveNextAsync());
        Assert.Empty(cursor.Current);
    }

    #endregion
}
