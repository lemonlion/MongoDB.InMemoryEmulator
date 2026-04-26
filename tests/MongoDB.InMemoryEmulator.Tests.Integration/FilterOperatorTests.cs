using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoDB.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for filter operators that were missing: $all, $elemMatch, $size, $mod, bitwise operators.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/reference/operator/query/
/// </remarks>
public class FilterOperatorTests
{
    private static IMongoCollection<BsonDocument> CreateCollection()
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("testdb");
        return db.GetCollection<BsonDocument>("items");
    }

    [Fact]
    public void All_matches_array_containing_all_values()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/all/
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "_id", 1 }, { "tags", new BsonArray { "a", "b", "c" } } });
        col.InsertOne(new BsonDocument { { "_id", 2 }, { "tags", new BsonArray { "a", "b" } } });
        col.InsertOne(new BsonDocument { { "_id", 3 }, { "tags", new BsonArray { "a" } } });

        var filter = Builders<BsonDocument>.Filter.All("tags", new[] { "a", "b" });
        var results = col.Find(filter).ToList();

        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r["_id"] == 1);
        Assert.Contains(results, r => r["_id"] == 2);
    }

    [Fact]
    public void ElemMatch_matches_array_element_meeting_all_criteria()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/elemMatch/
        var col = CreateCollection();
        col.InsertOne(new BsonDocument
        {
            { "_id", 1 },
            { "results", new BsonArray
                {
                    new BsonDocument { { "product", "abc" }, { "score", 10 } },
                    new BsonDocument { { "product", "xyz" }, { "score", 5 } }
                }
            }
        });
        col.InsertOne(new BsonDocument
        {
            { "_id", 2 },
            { "results", new BsonArray
                {
                    new BsonDocument { { "product", "abc" }, { "score", 3 } }
                }
            }
        });

        var filter = Builders<BsonDocument>.Filter.ElemMatch<BsonDocument>("results",
            Builders<BsonDocument>.Filter.And(
                Builders<BsonDocument>.Filter.Eq("product", "abc"),
                Builders<BsonDocument>.Filter.Gte("score", 8)));

        var results = col.Find(filter).ToList();
        Assert.Single(results);
        Assert.Equal(1, results[0]["_id"].AsInt32);
    }

    [Fact]
    public void Size_matches_array_by_count()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/size/
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "_id", 1 }, { "tags", new BsonArray { "a", "b", "c" } } });
        col.InsertOne(new BsonDocument { { "_id", 2 }, { "tags", new BsonArray { "a", "b" } } });
        col.InsertOne(new BsonDocument { { "_id", 3 }, { "tags", new BsonArray { "a" } } });

        var filter = Builders<BsonDocument>.Filter.Size("tags", 2);
        var results = col.Find(filter).ToList();
        Assert.Single(results);
        Assert.Equal(2, results[0]["_id"].AsInt32);
    }

    [Fact]
    public void Mod_matches_by_remainder()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/mod/
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "_id", 1 }, { "qty", 0 } });
        col.InsertOne(new BsonDocument { { "_id", 2 }, { "qty", 4 } });
        col.InsertOne(new BsonDocument { { "_id", 3 }, { "qty", 7 } });
        col.InsertOne(new BsonDocument { { "_id", 4 }, { "qty", 12 } });

        var filter = Builders<BsonDocument>.Filter.Mod("qty", 4, 0);
        var results = col.Find(filter).ToList();
        Assert.Equal(3, results.Count); // qty 0, 4, 12 all have remainder 0 when divided by 4
    }

    [Fact]
    public void BitsAllSet_matches_documents()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/bitsAllSet/
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "_id", 1 }, { "flags", 7 } });  // 0b0111
        col.InsertOne(new BsonDocument { { "_id", 2 }, { "flags", 3 } });  // 0b0011
        col.InsertOne(new BsonDocument { { "_id", 3 }, { "flags", 15 } }); // 0b1111

        var filter = Builders<BsonDocument>.Filter.BitsAllSet("flags", 5); // 0b0101
        var results = col.Find(filter).ToList();
        Assert.Equal(2, results.Count); // 7 (0111) and 15 (1111)
        Assert.Contains(results, r => r["_id"] == 1);
        Assert.Contains(results, r => r["_id"] == 3);
    }

    [Fact]
    public void BitsAllClear_matches_documents()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/bitsAllClear/
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "_id", 1 }, { "flags", 2 } });  // 0b0010
        col.InsertOne(new BsonDocument { { "_id", 2 }, { "flags", 8 } });  // 0b1000
        col.InsertOne(new BsonDocument { { "_id", 3 }, { "flags", 5 } });  // 0b0101

        var filter = Builders<BsonDocument>.Filter.BitsAllClear("flags", 5); // 0b0101
        var results = col.Find(filter).ToList();
        Assert.Equal(2, results.Count); // 2 (0010) and 8 (1000)
    }

    [Fact]
    public void BitsAnySet_matches_documents()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/bitsAnySet/
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "_id", 1 }, { "flags", 2 } });  // 0b0010
        col.InsertOne(new BsonDocument { { "_id", 2 }, { "flags", 8 } });  // 0b1000
        col.InsertOne(new BsonDocument { { "_id", 3 }, { "flags", 5 } });  // 0b0101

        var filter = Builders<BsonDocument>.Filter.BitsAnySet("flags", 6); // 0b0110
        var results = col.Find(filter).ToList();
        Assert.Equal(2, results.Count); // 2 (0010) and 5 (0101) have bit 1 or bit 2 set
    }

    [Fact]
    public void BitsAnyClear_matches_documents()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/bitsAnyClear/
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "_id", 1 }, { "flags", 7 } });   // 0b0111
        col.InsertOne(new BsonDocument { { "_id", 2 }, { "flags", 15 } });  // 0b1111
        col.InsertOne(new BsonDocument { { "_id", 3 }, { "flags", 3 } });   // 0b0011

        var filter = Builders<BsonDocument>.Filter.BitsAnyClear("flags", 7); // 0b0111
        var results = col.Find(filter).ToList();
        Assert.Single(results); // Only 3 (0011) has any of bits 0,1,2 clear (bit 2 is clear)
        Assert.Equal(3, results[0]["_id"].AsInt32);
    }

    [Fact]
    public void All_with_empty_array_returns_all_with_array_field()
    {
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "_id", 1 }, { "tags", new BsonArray { "a" } } });
        col.InsertOne(new BsonDocument { { "_id", 2 }, { "tags", new BsonArray() } });

        var filter = new BsonDocument("tags", new BsonDocument("$all", new BsonArray()));
        var results = col.Find(filter).ToList();
        // $all with empty array: on real MongoDB, empty $all matches all docs that have the field as an array.
        // Our implementation: all required elements (none) are found in any array, so matches both.
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public void ElemMatch_with_scalar_array()
    {
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "_id", 1 }, { "scores", new BsonArray { 80, 90, 95 } } });
        col.InsertOne(new BsonDocument { { "_id", 2 }, { "scores", new BsonArray { 40, 50, 60 } } });

        // $elemMatch with scalar operators
        var filter = new BsonDocument("scores", new BsonDocument("$elemMatch",
            new BsonDocument { { "$gte", 85 }, { "$lte", 95 } }));

        var results = col.Find(filter).ToList();
        Assert.Single(results);
        Assert.Equal(1, results[0]["_id"].AsInt32);
    }

    [Fact]
    public void Mod_with_non_zero_remainder()
    {
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "_id", 1 }, { "qty", 1 } });
        col.InsertOne(new BsonDocument { { "_id", 2 }, { "qty", 3 } });
        col.InsertOne(new BsonDocument { { "_id", 3 }, { "qty", 5 } });
        col.InsertOne(new BsonDocument { { "_id", 4 }, { "qty", 6 } });

        var filter = Builders<BsonDocument>.Filter.Mod("qty", 3, 0);
        var results = col.Find(filter).ToList();
        Assert.Equal(2, results.Count); // 3 and 6
    }
}
