using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoDB.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for expression operators added in Phase 5:
/// $binarySize, $bsonSize, $rand, $toHashedIndexKey, $tsIncrement, $tsSecond
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/
/// </remarks>
public class ExpressionOperatorTests
{
    private static IMongoCollection<BsonDocument> CreateCollection()
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("testdb");
        return db.GetCollection<BsonDocument>("items");
    }

    [Fact]
    public void BinarySize_returns_utf8_byte_count()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/binarySize/
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "_id", 1 }, { "text", "hello" } });
        col.InsertOne(new BsonDocument { { "_id", 2 }, { "text", "héllo" } }); // é is 2 bytes in UTF-8

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$project", new BsonDocument
            {
                { "size", new BsonDocument("$binarySize", "$text") }
            }));

        var results = col.Aggregate(pipeline).ToList();
        var doc1 = results.First(r => r["_id"] == 1);
        var doc2 = results.First(r => r["_id"] == 2);

        Assert.Equal(5, doc1["size"].AsInt32); // "hello" = 5 bytes
        Assert.Equal(6, doc2["size"].AsInt32); // "héllo" = 6 bytes (é = 2 bytes)
    }

    [Fact]
    public void BsonSize_returns_document_size()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/bsonSize/
        var col = CreateCollection();
        var smallDoc = new BsonDocument { { "_id", 1 }, { "data", "a" } };
        var bigDoc = new BsonDocument { { "_id", 2 }, { "data", new string('x', 1000) } };
        col.InsertOne(smallDoc);
        col.InsertOne(bigDoc);

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$project", new BsonDocument
            {
                { "docSize", new BsonDocument("$bsonSize", "$$ROOT") }
            }));

        var results = col.Aggregate(pipeline).ToList();
        var s1 = results.First(r => r["_id"] == 1)["docSize"].AsInt32;
        var s2 = results.First(r => r["_id"] == 2)["docSize"].AsInt32;

        Assert.True(s1 > 0);
        Assert.True(s2 > s1);
    }

    [Fact]
    public void Rand_returns_value_between_0_and_1()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/rand/
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "_id", 1 } });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$project", new BsonDocument
            {
                { "random", new BsonDocument("$rand", new BsonDocument()) }
            }));

        var results = col.Aggregate(pipeline).ToList();
        var val = results[0]["random"].ToDouble();
        Assert.True(val >= 0.0 && val < 1.0, $"Expected 0 <= {val} < 1");
    }

    [Fact]
    public void ToHashedIndexKey_returns_long()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/toHashedIndexKey/
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "_id", 1 }, { "val", "test" } });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$project", new BsonDocument
            {
                { "hashed", new BsonDocument("$toHashedIndexKey", "$val") }
            }));

        var results = col.Aggregate(pipeline).ToList();
        Assert.True(results[0]["hashed"].IsInt64 || results[0]["hashed"].IsNumeric);
    }

    [Fact]
    public void TsIncrement_extracts_increment_from_timestamp()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/tsIncrement/
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "_id", 1 }, { "ts", new BsonTimestamp(1000, 42) } });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$project", new BsonDocument
            {
                { "inc", new BsonDocument("$tsIncrement", "$ts") }
            }));

        var results = col.Aggregate(pipeline).ToList();
        Assert.Equal(42L, results[0]["inc"].AsInt64);
    }

    [Fact]
    public void TsSecond_extracts_seconds_from_timestamp()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/tsSecond/
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "_id", 1 }, { "ts", new BsonTimestamp(1000, 42) } });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$project", new BsonDocument
            {
                { "sec", new BsonDocument("$tsSecond", "$ts") }
            }));

        var results = col.Aggregate(pipeline).ToList();
        Assert.Equal(1000L, results[0]["sec"].AsInt64);
    }

    [Fact]
    public void BinarySize_null_returns_null()
    {
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "_id", 1 }, { "text", BsonNull.Value } });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$project", new BsonDocument
            {
                { "size", new BsonDocument("$binarySize", "$text") }
            }));

        var results = col.Aggregate(pipeline).ToList();
        Assert.Equal(BsonNull.Value, results[0]["size"]);
    }

    [Fact]
    public void TsIncrement_non_timestamp_returns_null()
    {
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "_id", 1 }, { "val", "not a timestamp" } });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$project", new BsonDocument
            {
                { "inc", new BsonDocument("$tsIncrement", "$val") }
            }));

        var results = col.Aggregate(pipeline).ToList();
        Assert.Equal(BsonNull.Value, results[0]["inc"]);
    }
}
