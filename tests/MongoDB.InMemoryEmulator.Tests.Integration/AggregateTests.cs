using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Linq;
using Xunit;

namespace MongoDB.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for aggregation pipeline stages ($match, $project, $group, $sort, $limit, $skip,
/// $unwind, $addFields/$set, $unset, $replaceRoot, $replaceWith, $count, $sortByCount, $sample).
/// </summary>
[Collection("Integration")]
public class AggregateTests : IAsyncLifetime
{
    private readonly MongoDbSession _session;
    private ITestCollectionFixture _fixture = null!;

    public AggregateTests(MongoDbSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        await _fixture.ResetAsync();
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    #region $match

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Match_FiltersDocuments()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_match_1");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "status", "active" }, { "score", 10 } },
            new BsonDocument { { "status", "inactive" }, { "score", 20 } },
            new BsonDocument { { "status", "active" }, { "score", 30 } },
        });

        var result = await col.Aggregate()
            .Match(Builders<BsonDocument>.Filter.Eq("status", "active"))
            .ToListAsync();

        Assert.Equal(2, result.Count);
        Assert.All(result, d => Assert.Equal("active", d["status"].AsString));
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Match_WithGtFilter()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_match_2");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "value", 5 } },
            new BsonDocument { { "value", 15 } },
            new BsonDocument { { "value", 25 } },
        });

        var result = await col.Aggregate()
            .Match(Builders<BsonDocument>.Filter.Gt("value", 10))
            .ToListAsync();

        Assert.Equal(2, result.Count);
    }

    #endregion

    #region $project

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Project_IncludesSpecifiedFields()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_proj_1");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "name", "Alice" }, { "age", 30 }, { "email", "alice@test.com" } },
        });

        var result = await col.Aggregate()
            .Project(new BsonDocument { { "name", 1 }, { "_id", 0 } })
            .ToListAsync();

        Assert.Single(result);
        Assert.Equal("Alice", result[0]["name"].AsString);
        Assert.False(result[0].Contains("age"));
        Assert.False(result[0].Contains("_id"));
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Project_ExcludesSpecifiedFields()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_proj_2");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "name", "Bob" }, { "age", 25 }, { "email", "bob@test.com" } },
        });

        var result = await col.Aggregate()
            .Project(new BsonDocument { { "email", 0 } })
            .ToListAsync();

        Assert.Single(result);
        Assert.True(result[0].Contains("name"));
        Assert.True(result[0].Contains("age"));
        Assert.False(result[0].Contains("email"));
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Project_ComputedFieldsWithExpression()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_proj_3");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "first", "John" }, { "last", "Doe" } },
        });

        var result = await col.Aggregate()
            .Project(new BsonDocument
            {
                { "_id", 0 },
                { "fullName", new BsonDocument("$concat", new BsonArray { "$first", " ", "$last" }) }
            })
            .ToListAsync();

        Assert.Single(result);
        Assert.Equal("John Doe", result[0]["fullName"].AsString);
    }

    #endregion

    #region $group

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Group_ByFieldWithSum()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_group_1");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "category", "A" }, { "amount", 10 } },
            new BsonDocument { { "category", "B" }, { "amount", 20 } },
            new BsonDocument { { "category", "A" }, { "amount", 30 } },
            new BsonDocument { { "category", "B" }, { "amount", 40 } },
        });

        var result = await col.Aggregate()
            .Group(new BsonDocument
            {
                { "_id", "$category" },
                { "total", new BsonDocument("$sum", "$amount") }
            })
            .ToListAsync();

        Assert.Equal(2, result.Count);
        var groupA = result.First(d => d["_id"].AsString == "A");
        var groupB = result.First(d => d["_id"].AsString == "B");
        Assert.Equal(40, groupA["total"].ToDouble());
        Assert.Equal(60, groupB["total"].ToDouble());
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Group_WithAvg()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_group_2");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "type", "X" }, { "score", 80 } },
            new BsonDocument { { "type", "X" }, { "score", 90 } },
            new BsonDocument { { "type", "Y" }, { "score", 70 } },
        });

        var result = await col.Aggregate()
            .Group(new BsonDocument
            {
                { "_id", "$type" },
                { "avgScore", new BsonDocument("$avg", "$score") }
            })
            .ToListAsync();

        var groupX = result.First(d => d["_id"].AsString == "X");
        Assert.Equal(85, groupX["avgScore"].ToDouble());
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Group_WithMinMaxFirstLast()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_group_3");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "g", 1 }, { "v", 5 } },
            new BsonDocument { { "g", 1 }, { "v", 1 } },
            new BsonDocument { { "g", 1 }, { "v", 9 } },
        });

        var result = await col.Aggregate()
            .Group(new BsonDocument
            {
                { "_id", "$g" },
                { "minV", new BsonDocument("$min", "$v") },
                { "maxV", new BsonDocument("$max", "$v") },
                { "firstV", new BsonDocument("$first", "$v") },
                { "lastV", new BsonDocument("$last", "$v") }
            })
            .ToListAsync();

        Assert.Single(result);
        Assert.Equal(1, result[0]["minV"].ToDouble());
        Assert.Equal(9, result[0]["maxV"].ToDouble());
        Assert.Equal(5, result[0]["firstV"].ToDouble());
        Assert.Equal(9, result[0]["lastV"].ToDouble());
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Group_WithPushAndAddToSet()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_group_4");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "g", 1 }, { "tag", "a" } },
            new BsonDocument { { "g", 1 }, { "tag", "b" } },
            new BsonDocument { { "g", 1 }, { "tag", "a" } },
        });

        var result = await col.Aggregate()
            .Group(new BsonDocument
            {
                { "_id", "$g" },
                { "allTags", new BsonDocument("$push", "$tag") },
                { "uniqueTags", new BsonDocument("$addToSet", "$tag") }
            })
            .ToListAsync();

        Assert.Single(result);
        Assert.Equal(3, result[0]["allTags"].AsBsonArray.Count);
        Assert.Equal(2, result[0]["uniqueTags"].AsBsonArray.Count);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Group_CountAccumulator()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_group_5");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "status", "A" } },
            new BsonDocument { { "status", "A" } },
            new BsonDocument { { "status", "B" } },
        });

        var result = await col.Aggregate()
            .Group(new BsonDocument
            {
                { "_id", "$status" },
                { "n", new BsonDocument("$count", new BsonDocument()) }
            })
            .ToListAsync();

        var groupA = result.First(d => d["_id"].AsString == "A");
        Assert.Equal(2, groupA["n"].AsInt32);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Group_NullId_AggregatesAll()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_group_6");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "v", 10 } },
            new BsonDocument { { "v", 20 } },
            new BsonDocument { { "v", 30 } },
        });

        var result = await col.Aggregate()
            .Group(new BsonDocument
            {
                { "_id", BsonNull.Value },
                { "total", new BsonDocument("$sum", "$v") }
            })
            .ToListAsync();

        Assert.Single(result);
        Assert.Equal(60, result[0]["total"].ToDouble());
    }

    #endregion

    #region $sort / $limit / $skip

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Sort_AscendingOrder()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_sort_1");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "n", 3 } },
            new BsonDocument { { "n", 1 } },
            new BsonDocument { { "n", 2 } },
        });

        var result = await col.Aggregate()
            .Sort(new BsonDocument("n", 1))
            .ToListAsync();

        Assert.Equal(1, result[0]["n"].AsInt32);
        Assert.Equal(2, result[1]["n"].AsInt32);
        Assert.Equal(3, result[2]["n"].AsInt32);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Limit_ReturnsSpecifiedCount()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_limit_1");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "n", 1 } },
            new BsonDocument { { "n", 2 } },
            new BsonDocument { { "n", 3 } },
        });

        var result = await col.Aggregate()
            .Sort(new BsonDocument("n", 1))
            .Limit(2)
            .ToListAsync();

        Assert.Equal(2, result.Count);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Skip_SkipsDocuments()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_skip_1");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "n", 1 } },
            new BsonDocument { { "n", 2 } },
            new BsonDocument { { "n", 3 } },
        });

        var result = await col.Aggregate()
            .Sort(new BsonDocument("n", 1))
            .Skip(1)
            .ToListAsync();

        Assert.Equal(2, result.Count);
        Assert.Equal(2, result[0]["n"].AsInt32);
    }

    #endregion

    #region $unwind

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Unwind_FlattensArray()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_unwind_1");
        await col.InsertOneAsync(new BsonDocument
        {
            { "name", "item" },
            { "tags", new BsonArray { "a", "b", "c" } }
        });

        var result = await col.Aggregate()
            .Unwind("tags")
            .ToListAsync();

        Assert.Equal(3, result.Count);
        Assert.Equal("a", result[0]["tags"].AsString);
        Assert.Equal("b", result[1]["tags"].AsString);
        Assert.Equal("c", result[2]["tags"].AsString);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Unwind_PreserveNullAndEmpty()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_unwind_2");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "name", "a" }, { "arr", new BsonArray { 1 } } },
            new BsonDocument { { "name", "b" }, { "arr", new BsonArray() } },
            new BsonDocument { { "name", "c" } }, // missing arr
        });

        var result = await col.Aggregate()
            .Unwind("arr", new AggregateUnwindOptions<BsonDocument> { PreserveNullAndEmptyArrays = true })
            .ToListAsync();

        Assert.Equal(3, result.Count);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Unwind_WithIncludeArrayIndex()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_unwind_3");
        await col.InsertOneAsync(new BsonDocument
        {
            { "items", new BsonArray { "x", "y" } }
        });

        var result = await col.Aggregate()
            .Unwind("items", new AggregateUnwindOptions<BsonDocument> { IncludeArrayIndex = "idx" })
            .ToListAsync();

        Assert.Equal(2, result.Count);
        Assert.Equal(0, result[0]["idx"].ToInt64());
        Assert.Equal(1, result[1]["idx"].ToInt64());
    }

    #endregion

    #region $addFields / $set / $unset

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task AddFields_AddsNewField()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_addfields_1");
        await col.InsertOneAsync(new BsonDocument { { "a", 10 }, { "b", 20 } });

        var result = await col.Aggregate()
            .AppendStage<BsonDocument>(new BsonDocument("$addFields", new BsonDocument("sum", new BsonDocument("$add", new BsonArray { "$a", "$b" }))))
            .ToListAsync();

        Assert.Single(result);
        Assert.Equal(30, result[0]["sum"].ToDouble());
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Set_IsAliasForAddFields()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_set_1");
        await col.InsertOneAsync(new BsonDocument { { "x", 5 } });

        var result = await col.Aggregate()
            .AppendStage<BsonDocument>(new BsonDocument("$set", new BsonDocument("doubled", new BsonDocument("$multiply", new BsonArray { "$x", 2 }))))
            .ToListAsync();

        Assert.Single(result);
        Assert.Equal(10, result[0]["doubled"].ToDouble());
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Unset_RemovesFields()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_unset_1");
        await col.InsertOneAsync(new BsonDocument { { "keep", 1 }, { "remove", 2 } });

        var result = await col.Aggregate()
            .AppendStage<BsonDocument>(new BsonDocument("$unset", "remove"))
            .ToListAsync();

        Assert.Single(result);
        Assert.True(result[0].Contains("keep"));
        Assert.False(result[0].Contains("remove"));
    }

    #endregion

    #region $replaceRoot / $replaceWith

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task ReplaceRoot_PromotesSubdocument()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_rroot_1");
        await col.InsertOneAsync(new BsonDocument
        {
            { "name", "outer" },
            { "inner", new BsonDocument { { "a", 1 }, { "b", 2 } } }
        });

        var result = await col.Aggregate()
            .ReplaceRoot<BsonDocument>("$inner")
            .ToListAsync();

        Assert.Single(result);
        Assert.Equal(1, result[0]["a"].AsInt32);
        Assert.Equal(2, result[0]["b"].AsInt32);
        Assert.False(result[0].Contains("name"));
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task ReplaceWith_PromotesSubdocument()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_rwith_1");
        await col.InsertOneAsync(new BsonDocument
        {
            { "nested", new BsonDocument { { "val", 42 } } }
        });

        var result = await col.Aggregate()
            .AppendStage<BsonDocument>(new BsonDocument("$replaceWith", "$nested"))
            .ToListAsync();

        Assert.Single(result);
        Assert.Equal(42, result[0]["val"].AsInt32);
    }

    #endregion

    #region $count

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Count_ReturnsDocumentCount()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_count_1");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "x", 1 } },
            new BsonDocument { { "x", 2 } },
            new BsonDocument { { "x", 3 } },
        });

        var result = await col.Aggregate()
            .Count()
            .ToListAsync();

        Assert.Single(result);
        Assert.Equal(3, result[0].Count);
    }

    #endregion

    #region $sortByCount

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task SortByCount_GroupsAndSorts()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_sbc_1");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "color", "red" } },
            new BsonDocument { { "color", "blue" } },
            new BsonDocument { { "color", "red" } },
            new BsonDocument { { "color", "red" } },
            new BsonDocument { { "color", "blue" } },
        });

        var result = await col.Aggregate()
            .SortByCount<BsonValue>("$color")
            .ToListAsync();

        Assert.Equal(2, result.Count);
        Assert.Equal("red", result[0].Id.AsString);
        Assert.Equal(3, result[0].Count);
        Assert.Equal("blue", result[1].Id.AsString);
        Assert.Equal(2, result[1].Count);
    }

    #endregion

    #region $sample

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Sample_ReturnsRequestedCount()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_sample_1");
        await col.InsertManyAsync(Enumerable.Range(1, 10).Select(i => new BsonDocument("n", i)));

        var result = await col.Aggregate()
            .AppendStage<BsonDocument>(new BsonDocument("$sample", new BsonDocument("size", 3)))
            .ToListAsync();

        Assert.Equal(3, result.Count);
    }

    #endregion

    #region Multi-stage pipelines

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task MultiStage_MatchGroupSort()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_multi_1");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "dept", "eng" }, { "salary", 80000 } },
            new BsonDocument { { "dept", "eng" }, { "salary", 90000 } },
            new BsonDocument { { "dept", "sales" }, { "salary", 60000 } },
            new BsonDocument { { "dept", "sales" }, { "salary", 70000 } },
            new BsonDocument { { "dept", "hr" }, { "salary", 50000 } },
        });

        var result = await col.Aggregate()
            .Match(Builders<BsonDocument>.Filter.Ne("dept", "hr"))
            .Group(new BsonDocument
            {
                { "_id", "$dept" },
                { "totalSalary", new BsonDocument("$sum", "$salary") }
            })
            .Sort(new BsonDocument("totalSalary", -1))
            .ToListAsync();

        Assert.Equal(2, result.Count);
        Assert.Equal("eng", result[0]["_id"].AsString);
        Assert.Equal(170000, result[0]["totalSalary"].ToDouble());
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task MultiStage_MatchProjectSortSkipLimit()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_multi_2");
        await col.InsertManyAsync(Enumerable.Range(1, 20).Select(i =>
            new BsonDocument { { "n", i }, { "even", i % 2 == 0 } }));

        var result = await col.Aggregate()
            .Match(Builders<BsonDocument>.Filter.Eq("even", true))
            .Project(new BsonDocument { { "_id", 0 }, { "n", 1 } })
            .Sort(new BsonDocument("n", 1))
            .Skip(2)
            .Limit(3)
            .ToListAsync();

        Assert.Equal(3, result.Count);
        Assert.Equal(6, result[0]["n"].AsInt32);
        Assert.Equal(8, result[1]["n"].AsInt32);
        Assert.Equal(10, result[2]["n"].AsInt32);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task MultiStage_UnwindGroup()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_multi_3");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "product", "A" }, { "tags", new BsonArray { "electronics", "sale" } } },
            new BsonDocument { { "product", "B" }, { "tags", new BsonArray { "sale", "clearance" } } },
        });

        var result = await col.Aggregate()
            .Unwind("tags")
            .Group(new BsonDocument
            {
                { "_id", "$tags" },
                { "count", new BsonDocument("$sum", 1) }
            })
            .Sort(new BsonDocument("count", -1))
            .ToListAsync();

        var saleGroup = result.First(d => d["_id"].AsString == "sale");
        Assert.Equal(2, saleGroup["count"].ToInt64());
    }

    #endregion

    #region $facet

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Facet_MultipleSubPipelines()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_facet_1");
        await col.InsertManyAsync(Enumerable.Range(1, 10).Select(i => new BsonDocument("n", i)));

        var result = await col.Aggregate()
            .AppendStage<BsonDocument>(new BsonDocument("$facet", new BsonDocument
            {
                { "low", new BsonArray { new BsonDocument("$match", new BsonDocument("n", new BsonDocument("$lte", 3))) } },
                { "high", new BsonArray { new BsonDocument("$match", new BsonDocument("n", new BsonDocument("$gte", 8))) } }
            }))
            .ToListAsync();

        Assert.Single(result);
        Assert.Equal(3, result[0]["low"].AsBsonArray.Count);
        Assert.Equal(3, result[0]["high"].AsBsonArray.Count);
    }

    #endregion

    #region $bucket

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Bucket_GroupsIntoBoundaries()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_bucket_1");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "age", 15 } },
            new BsonDocument { { "age", 25 } },
            new BsonDocument { { "age", 35 } },
            new BsonDocument { { "age", 45 } },
            new BsonDocument { { "age", 55 } },
        });

        var result = await col.Aggregate()
            .AppendStage<BsonDocument>(new BsonDocument("$bucket", new BsonDocument
            {
                { "groupBy", "$age" },
                { "boundaries", new BsonArray { 0, 20, 40, 60 } },
            }))
            .ToListAsync();

        Assert.Equal(3, result.Count);
        Assert.Equal(0, result[0]["_id"].AsInt32);
        Assert.Equal(1, result[0]["count"].AsInt32);
        Assert.Equal(20, result[1]["_id"].AsInt32);
        Assert.Equal(2, result[1]["count"].AsInt32);
    }

    #endregion

    #region Typed aggregation (POCO)

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Aggregate_TypedPipeline_MatchAndProject()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_typed_1");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "name", "Alice" }, { "score", 95 } },
            new BsonDocument { { "name", "Bob" }, { "score", 42 } },
            new BsonDocument { { "name", "Charlie" }, { "score", 88 } },
        });

        var result = await col.Aggregate()
            .Match(Builders<BsonDocument>.Filter.Gte("score", 50))
            .Project(new BsonDocument { { "_id", 0 }, { "name", 1 } })
            .ToListAsync();

        Assert.Equal(2, result.Count);
        Assert.Contains(result, d => d["name"].AsString == "Alice");
        Assert.Contains(result, d => d["name"].AsString == "Charlie");
    }

    #endregion

    #region Empty pipeline

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Aggregate_EmptyPipeline_ReturnsAllDocuments()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_empty_1");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "x", 1 } },
            new BsonDocument { { "x", 2 } },
        });

        var result = await col.Aggregate()
            .ToListAsync();

        Assert.Equal(2, result.Count);
    }

    #endregion

    #region $redact

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Redact_PrunesDocuments()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_redact_1");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "level", "public" }, { "data", "ok" } },
            new BsonDocument { { "level", "secret" }, { "data", "hidden" } },
        });

        var result = await col.Aggregate()
            .AppendStage<BsonDocument>(new BsonDocument("$redact", new BsonDocument(
                "$cond", new BsonDocument
                {
                    { "if", new BsonDocument("$eq", new BsonArray { "$level", "public" }) },
                    { "then", "$$KEEP" },
                    { "else", "$$PRUNE" }
                })))
            .ToListAsync();

        Assert.Single(result);
        Assert.Equal("ok", result[0]["data"].AsString);
    }

    #endregion

    #region $lookup

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Lookup_SimpleEquality_JoinsCollections()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/lookup/
        var orders = _fixture.GetCollection<BsonDocument>("agg_lookup_orders");
        var inventory = _fixture.GetCollection<BsonDocument>("agg_lookup_inventory");

        await orders.InsertManyAsync(new[]
        {
            new BsonDocument { { "item", "apple" }, { "qty", 5 } },
            new BsonDocument { { "item", "banana" }, { "qty", 10 } },
        });
        await inventory.InsertManyAsync(new[]
        {
            new BsonDocument { { "sku", "apple" }, { "warehouse", "A" } },
            new BsonDocument { { "sku", "apple" }, { "warehouse", "B" } },
            new BsonDocument { { "sku", "banana" }, { "warehouse", "C" } },
        });

        var result = await orders.Aggregate()
            .Lookup("agg_lookup_inventory", "item", "sku", "inventory_docs")
            .ToListAsync();

        Assert.Equal(2, result.Count);
        var appleDoc = result.First(d => d["item"] == "apple");
        var bananaDoc = result.First(d => d["item"] == "banana");
        Assert.Equal(2, appleDoc["inventory_docs"].AsBsonArray.Count);
        Assert.Single(bananaDoc["inventory_docs"].AsBsonArray);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Lookup_PipelineForm_WithLetVariables()
    {
        var orders = _fixture.GetCollection<BsonDocument>("agg_lookup_pipe_orders");
        var items = _fixture.GetCollection<BsonDocument>("agg_lookup_pipe_items");

        await orders.InsertManyAsync(new[]
        {
            new BsonDocument { { "item", "apple" }, { "price", 5.0 } },
            new BsonDocument { { "item", "banana" }, { "price", 3.0 } },
        });
        await items.InsertManyAsync(new[]
        {
            new BsonDocument { { "name", "apple" }, { "instock", 100 } },
            new BsonDocument { { "name", "banana" }, { "instock", 200 } },
            new BsonDocument { { "name", "cherry" }, { "instock", 50 } },
        });

        var result = await orders.Aggregate()
            .AppendStage<BsonDocument>(new BsonDocument("$lookup", new BsonDocument
            {
                { "from", "agg_lookup_pipe_items" },
                { "let", new BsonDocument("order_item", "$item") },
                { "pipeline", new BsonArray
                    {
                        new BsonDocument("$match", new BsonDocument("$expr",
                            new BsonDocument("$eq", new BsonArray { "$name", "$$order_item" })))
                    }
                },
                { "as", "matched_items" }
            }))
            .ToListAsync();

        Assert.Equal(2, result.Count);
        var appleResult = result.First(d => d["item"] == "apple");
        Assert.Single(appleResult["matched_items"].AsBsonArray);
        Assert.Equal("apple", appleResult["matched_items"][0]["name"].AsString);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Lookup_NoMatch_ReturnsEmptyArray()
    {
        var orders = _fixture.GetCollection<BsonDocument>("agg_lookup_nomatch_orders");
        var products = _fixture.GetCollection<BsonDocument>("agg_lookup_nomatch_products");

        await orders.InsertOneAsync(new BsonDocument { { "item", "grape" }, { "qty", 1 } });
        await products.InsertOneAsync(new BsonDocument { { "sku", "apple" }, { "warehouse", "A" } });

        var result = await orders.Aggregate()
            .Lookup("agg_lookup_nomatch_products", "item", "sku", "matched")
            .ToListAsync();

        Assert.Single(result);
        Assert.Empty(result[0]["matched"].AsBsonArray);
    }

    #endregion

    #region $unionWith

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task UnionWith_CombinesTwoCollections()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/unionWith/
        var sales2020 = _fixture.GetCollection<BsonDocument>("agg_union_sales2020");
        var sales2021 = _fixture.GetCollection<BsonDocument>("agg_union_sales2021");

        await sales2020.InsertManyAsync(new[]
        {
            new BsonDocument { { "item", "apple" }, { "amount", 100 } },
            new BsonDocument { { "item", "banana" }, { "amount", 200 } },
        });
        await sales2021.InsertManyAsync(new[]
        {
            new BsonDocument { { "item", "apple" }, { "amount", 150 } },
            new BsonDocument { { "item", "cherry" }, { "amount", 300 } },
        });

        var result = await sales2020.Aggregate()
            .AppendStage<BsonDocument>(new BsonDocument("$unionWith", new BsonDocument
            {
                { "coll", "agg_union_sales2021" }
            }))
            .ToListAsync();

        Assert.Equal(4, result.Count);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task UnionWith_WithPipeline_FiltersBeforeUnion()
    {
        var col1 = _fixture.GetCollection<BsonDocument>("agg_union_pipe_1");
        var col2 = _fixture.GetCollection<BsonDocument>("agg_union_pipe_2");

        await col1.InsertManyAsync(new[]
        {
            new BsonDocument { { "type", "A" }, { "val", 1 } },
            new BsonDocument { { "type", "B" }, { "val", 2 } },
        });
        await col2.InsertManyAsync(new[]
        {
            new BsonDocument { { "type", "A" }, { "val", 3 } },
            new BsonDocument { { "type", "C" }, { "val", 4 } },
        });

        var result = await col1.Aggregate()
            .AppendStage<BsonDocument>(new BsonDocument("$unionWith", new BsonDocument
            {
                { "coll", "agg_union_pipe_2" },
                { "pipeline", new BsonArray
                    {
                        new BsonDocument("$match", new BsonDocument("type", "A"))
                    }
                }
            }))
            .ToListAsync();

        // 2 from col1 + 1 matching from col2
        Assert.Equal(3, result.Count);
    }

    #endregion

    #region $graphLookup

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task GraphLookup_TraversesHierarchy()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/graphLookup/
        var employees = _fixture.GetCollection<BsonDocument>("agg_graphlookup_emp");

        await employees.InsertManyAsync(new[]
        {
            new BsonDocument { { "name", "Dev" }, { "reportsTo", "Eliot" } },
            new BsonDocument { { "name", "Eliot" }, { "reportsTo", "Ron" } },
            new BsonDocument { { "name", "Ron" }, { "reportsTo", BsonNull.Value } },
            new BsonDocument { { "name", "Andrew" }, { "reportsTo", "Eliot" } },
        });

        var result = await employees.Aggregate()
            .AppendStage<BsonDocument>(new BsonDocument("$graphLookup", new BsonDocument
            {
                { "from", "agg_graphlookup_emp" },
                { "startWith", "$reportsTo" },
                { "connectFromField", "reportsTo" },
                { "connectToField", "name" },
                { "as", "reportingHierarchy" },
                { "depthField", "depth" }
            }))
            .ToListAsync();

        var dev = result.First(d => d["name"] == "Dev");
        var hierarchy = dev["reportingHierarchy"].AsBsonArray;
        // Dev -> Eliot -> Ron (2 levels)
        Assert.Equal(2, hierarchy.Count);
    }

    #endregion

    #region $merge / $out

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Merge_WritesResultsToCollection()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/merge/
        var source = _fixture.GetCollection<BsonDocument>("agg_merge_source");
        var target = _fixture.GetCollection<BsonDocument>("agg_merge_target");

        await source.InsertManyAsync(new[]
        {
            new BsonDocument { { "_id", 1 }, { "category", "A" }, { "amount", 100 } },
            new BsonDocument { { "_id", 2 }, { "category", "A" }, { "amount", 200 } },
            new BsonDocument { { "_id", 3 }, { "category", "B" }, { "amount", 300 } },
        });

        await source.Aggregate()
            .Group(new BsonDocument { { "_id", "$category" }, { "total", new BsonDocument("$sum", "$amount") } })
            .AppendStage<BsonDocument>(new BsonDocument("$merge", new BsonDocument
            {
                { "into", "agg_merge_target" },
                { "whenMatched", "replace" },
                { "whenNotMatched", "insert" }
            }))
            .ToListAsync();

        var results = await target.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();
        Assert.Equal(2, results.Count);
        var catA = results.First(d => d["_id"] == "A");
        Assert.Equal(300, catA["total"].ToInt32());
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Out_WritesResultsToNewCollection()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/out/
        var source = _fixture.GetCollection<BsonDocument>("agg_out_source");
        var outputCol = _fixture.GetCollection<BsonDocument>("agg_out_dest");

        await source.InsertManyAsync(new[]
        {
            new BsonDocument { { "item", "apple" }, { "price", 5 } },
            new BsonDocument { { "item", "banana" }, { "price", 3 } },
        });

        await source.Aggregate()
            .Match(Builders<BsonDocument>.Filter.Gte("price", 4))
            .AppendStage<BsonDocument>(new BsonDocument("$out", "agg_out_dest"))
            .ToListAsync();

        var results = await outputCol.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();
        Assert.Single(results);
        Assert.Equal("apple", results[0]["item"].AsString);
    }

    #endregion

    #region Expression operators in $project

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Expression_Arithmetic_AddAndMultiply()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_expr_arith");
        await col.InsertOneAsync(new BsonDocument { { "a", 10 }, { "b", 5 } });

        var result = await col.Aggregate()
            .AppendStage<BsonDocument>(new BsonDocument("$project", new BsonDocument
            {
                { "_id", 0 },
                { "sum", new BsonDocument("$add", new BsonArray { "$a", "$b" }) },
                { "product", new BsonDocument("$multiply", new BsonArray { "$a", "$b" }) },
                { "diff", new BsonDocument("$subtract", new BsonArray { "$a", "$b" }) },
                { "quotient", new BsonDocument("$divide", new BsonArray { "$a", "$b" }) },
                { "remainder", new BsonDocument("$mod", new BsonArray { "$a", "$b" }) },
            }))
            .ToListAsync();

        Assert.Single(result);
        Assert.Equal(15.0, result[0]["sum"].ToDouble());
        Assert.Equal(50.0, result[0]["product"].ToDouble());
        Assert.Equal(5.0, result[0]["diff"].ToDouble());
        Assert.Equal(2.0, result[0]["quotient"].ToDouble());
        Assert.Equal(0.0, result[0]["remainder"].ToDouble());
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Expression_String_ConcatAndToLower()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_expr_string");
        await col.InsertOneAsync(new BsonDocument { { "first", "John" }, { "last", "DOE" } });

        var result = await col.Aggregate()
            .AppendStage<BsonDocument>(new BsonDocument("$project", new BsonDocument
            {
                { "_id", 0 },
                { "fullName", new BsonDocument("$concat", new BsonArray { "$first", " ", "$last" }) },
                { "lower", new BsonDocument("$toLower", "$last") },
                { "upper", new BsonDocument("$toUpper", "$first") },
            }))
            .ToListAsync();

        Assert.Single(result);
        Assert.Equal("John DOE", result[0]["fullName"].AsString);
        Assert.Equal("doe", result[0]["lower"].AsString);
        Assert.Equal("JOHN", result[0]["upper"].AsString);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Expression_Conditional_Cond()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_expr_cond");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "score", 85 } },
            new BsonDocument { { "score", 45 } },
        });

        var result = await col.Aggregate()
            .Sort(new BsonDocument("score", 1))
            .AppendStage<BsonDocument>(new BsonDocument("$project", new BsonDocument
            {
                { "grade", new BsonDocument("$cond", new BsonDocument
                    {
                        { "if", new BsonDocument("$gte", new BsonArray { "$score", 60 }) },
                        { "then", "pass" },
                        { "else", "fail" }
                    })
                }
            }))
            .ToListAsync();

        Assert.Equal("fail", result[0]["grade"].AsString);
        Assert.Equal("pass", result[1]["grade"].AsString);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Expression_IfNull_ProvidesDefault()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_expr_ifnull");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "name", "Alice" }, { "nickname", "Ali" } },
            new BsonDocument { { "name", "Bob" } }, // no nickname
        });

        var result = await col.Aggregate()
            .AppendStage<BsonDocument>(new BsonDocument("$project", new BsonDocument
            {
                { "displayName", new BsonDocument("$ifNull", new BsonArray { "$nickname", "$name" }) }
            }))
            .SortBy(d => d["displayName"])
            .ToListAsync();

        Assert.Equal(2, result.Count);
        Assert.Equal("Ali", result[0]["displayName"].AsString);
        Assert.Equal("Bob", result[1]["displayName"].AsString);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Expression_ArraySize()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_expr_arrsize");
        await col.InsertOneAsync(new BsonDocument { { "tags", new BsonArray { "a", "b", "c" } } });

        var result = await col.Aggregate()
            .AppendStage<BsonDocument>(new BsonDocument("$project", new BsonDocument
            {
                { "_id", 0 },
                { "tagCount", new BsonDocument("$size", "$tags") },
            }))
            .ToListAsync();

        Assert.Single(result);
        Assert.Equal(3, result[0]["tagCount"].ToInt32());
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Expression_ArrayElemAt()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_expr_elemAt");
        await col.InsertOneAsync(new BsonDocument { { "arr", new BsonArray { 10, 20, 30 } } });

        var result = await col.Aggregate()
            .AppendStage<BsonDocument>(new BsonDocument("$project", new BsonDocument
            {
                { "_id", 0 },
                { "first", new BsonDocument("$arrayElemAt", new BsonArray { "$arr", 0 }) },
                { "last", new BsonDocument("$arrayElemAt", new BsonArray { "$arr", -1 }) },
            }))
            .ToListAsync();

        Assert.Single(result);
        Assert.Equal(10, result[0]["first"].ToInt32());
        Assert.Equal(30, result[0]["last"].ToInt32());
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Expression_DateYear()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_expr_date");
        await col.InsertOneAsync(new BsonDocument { { "date", new BsonDateTime(new DateTime(2024, 6, 15, 10, 30, 0, DateTimeKind.Utc)) } });

        var result = await col.Aggregate()
            .AppendStage<BsonDocument>(new BsonDocument("$project", new BsonDocument
            {
                { "_id", 0 },
                { "year", new BsonDocument("$year", "$date") },
                { "month", new BsonDocument("$month", "$date") },
                { "day", new BsonDocument("$dayOfMonth", "$date") },
            }))
            .ToListAsync();

        Assert.Single(result);
        Assert.Equal(2024, result[0]["year"].ToInt32());
        Assert.Equal(6, result[0]["month"].ToInt32());
        Assert.Equal(15, result[0]["day"].ToInt32());
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Expression_TypeConversion()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_expr_type");
        await col.InsertOneAsync(new BsonDocument { { "val", "42" } });

        var result = await col.Aggregate()
            .AppendStage<BsonDocument>(new BsonDocument("$project", new BsonDocument
            {
                { "_id", 0 },
                { "asInt", new BsonDocument("$toInt", "$val") },
                { "asDouble", new BsonDocument("$toDouble", "$val") },
                { "typeName", new BsonDocument("$type", "$val") },
            }))
            .ToListAsync();

        Assert.Single(result);
        Assert.Equal(42, result[0]["asInt"].ToInt32());
        Assert.Equal(42.0, result[0]["asDouble"].ToDouble());
        Assert.Equal("string", result[0]["typeName"].AsString);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Expression_Switch()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_expr_switch");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "score", 95 } },
            new BsonDocument { { "score", 75 } },
            new BsonDocument { { "score", 35 } },
        });

        var result = await col.Aggregate()
            .Sort(new BsonDocument("score", 1))
            .AppendStage<BsonDocument>(new BsonDocument("$project", new BsonDocument
            {
                { "grade", new BsonDocument("$switch", new BsonDocument
                    {
                        { "branches", new BsonArray
                            {
                                new BsonDocument { { "case", new BsonDocument("$gte", new BsonArray { "$score", 90 }) }, { "then", "A" } },
                                new BsonDocument { { "case", new BsonDocument("$gte", new BsonArray { "$score", 60 }) }, { "then", "B" } },
                            }
                        },
                        { "default", "F" }
                    })
                }
            }))
            .ToListAsync();

        Assert.Equal("F", result[0]["grade"].AsString);
        Assert.Equal("B", result[1]["grade"].AsString);
        Assert.Equal("A", result[2]["grade"].AsString);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Expression_Filter_ArrayElements()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_expr_filter");
        await col.InsertOneAsync(new BsonDocument
        {
            { "items", new BsonArray
                {
                    new BsonDocument { { "name", "a" }, { "qty", 5 } },
                    new BsonDocument { { "name", "b" }, { "qty", 15 } },
                    new BsonDocument { { "name", "c" }, { "qty", 25 } },
                }
            }
        });

        var result = await col.Aggregate()
            .AppendStage<BsonDocument>(new BsonDocument("$project", new BsonDocument
            {
                { "_id", 0 },
                { "filtered", new BsonDocument("$filter", new BsonDocument
                    {
                        { "input", "$items" },
                        { "as", "item" },
                        { "cond", new BsonDocument("$gte", new BsonArray { "$$item.qty", 10 }) }
                    })
                }
            }))
            .ToListAsync();

        Assert.Single(result);
        Assert.Equal(2, result[0]["filtered"].AsBsonArray.Count);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Expression_Map_TransformsArray()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_expr_map");
        await col.InsertOneAsync(new BsonDocument { { "nums", new BsonArray { 1, 2, 3 } } });

        var result = await col.Aggregate()
            .AppendStage<BsonDocument>(new BsonDocument("$project", new BsonDocument
            {
                { "_id", 0 },
                { "doubled", new BsonDocument("$map", new BsonDocument
                    {
                        { "input", "$nums" },
                        { "as", "n" },
                        { "in", new BsonDocument("$multiply", new BsonArray { "$$n", 2 }) }
                    })
                }
            }))
            .ToListAsync();

        Assert.Single(result);
        var arr = result[0]["doubled"].AsBsonArray;
        Assert.Equal(3, arr.Count);
        Assert.Equal(2.0, arr[0].ToDouble());
        Assert.Equal(4.0, arr[1].ToDouble());
        Assert.Equal(6.0, arr[2].ToDouble());
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Expression_Let_DefinesLocalVariables()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_expr_let");
        await col.InsertOneAsync(new BsonDocument { { "price", 10 }, { "tax", 0.2 } });

        var result = await col.Aggregate()
            .AppendStage<BsonDocument>(new BsonDocument("$project", new BsonDocument
            {
                { "_id", 0 },
                { "total", new BsonDocument("$let", new BsonDocument
                    {
                        { "vars", new BsonDocument
                            {
                                { "total", new BsonDocument("$add", new BsonArray { "$price", new BsonDocument("$multiply", new BsonArray { "$price", "$tax" }) }) }
                            }
                        },
                        { "in", "$$total" }
                    })
                }
            }))
            .ToListAsync();

        Assert.Single(result);
        Assert.Equal(12.0, result[0]["total"].ToDouble());
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Expression_MergeObjects()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_expr_merge");
        await col.InsertOneAsync(new BsonDocument
        {
            { "defaults", new BsonDocument { { "color", "red" }, { "size", "M" } } },
            { "overrides", new BsonDocument { { "color", "blue" } } }
        });

        var result = await col.Aggregate()
            .AppendStage<BsonDocument>(new BsonDocument("$project", new BsonDocument
            {
                { "_id", 0 },
                { "merged", new BsonDocument("$mergeObjects", new BsonArray { "$defaults", "$overrides" }) }
            }))
            .ToListAsync();

        Assert.Single(result);
        Assert.Equal("blue", result[0]["merged"]["color"].AsString);
        Assert.Equal("M", result[0]["merged"]["size"].AsString);
    }

    #endregion

    #region $setWindowFields

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task SetWindowFields_Rank()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/setWindowFields/
        var col = _fixture.GetCollection<BsonDocument>("agg_window_rank");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "name", "Alice" }, { "score", 90 } },
            new BsonDocument { { "name", "Bob" }, { "score", 80 } },
            new BsonDocument { { "name", "Charlie" }, { "score", 90 } },
            new BsonDocument { { "name", "Diana" }, { "score", 70 } },
        });

        var result = await col.Aggregate()
            .AppendStage<BsonDocument>(new BsonDocument("$setWindowFields", new BsonDocument
            {
                { "sortBy", new BsonDocument("score", -1) },
                { "output", new BsonDocument
                    {
                        { "rank", new BsonDocument
                            {
                                { "$rank", new BsonDocument() }
                            }
                        }
                    }
                }
            }))
            .AppendStage<BsonDocument>(new BsonDocument("$sort", new BsonDocument("rank", 1)))
            .ToListAsync();

        Assert.Equal(4, result.Count);
        Assert.Equal(1, result[0]["rank"].ToInt32());
        Assert.Equal(1, result[1]["rank"].ToInt32()); // Tied with first
        Assert.Equal(3, result[2]["rank"].ToInt32()); // Rank skips to 3
        Assert.Equal(4, result[3]["rank"].ToInt32());
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task SetWindowFields_RunningSum()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_window_runsum");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "date", 1 }, { "amount", 10 } },
            new BsonDocument { { "date", 2 }, { "amount", 20 } },
            new BsonDocument { { "date", 3 }, { "amount", 30 } },
        });

        var result = await col.Aggregate()
            .AppendStage<BsonDocument>(new BsonDocument("$setWindowFields", new BsonDocument
            {
                { "sortBy", new BsonDocument("date", 1) },
                { "output", new BsonDocument
                    {
                        { "runningTotal", new BsonDocument
                            {
                                { "$sum", "$amount" },
                                { "window", new BsonDocument("documents", new BsonArray { "unbounded", "current" }) }
                            }
                        }
                    }
                }
            }))
            .AppendStage<BsonDocument>(new BsonDocument("$sort", new BsonDocument("date", 1)))
            .ToListAsync();

        Assert.Equal(3, result.Count);
        Assert.Equal(10.0, result[0]["runningTotal"].ToDouble());
        Assert.Equal(30.0, result[1]["runningTotal"].ToDouble());
        Assert.Equal(60.0, result[2]["runningTotal"].ToDouble());
    }

    #endregion

    #region LINQ / AsQueryable

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Linq_Where_FiltersDocuments()
    {
        var col = _fixture.GetCollection<LinqTestItem>("agg_linq_where");
        await col.InsertManyAsync(new[]
        {
            new LinqTestItem { Name = "Alice", Age = 30 },
            new LinqTestItem { Name = "Bob", Age = 25 },
            new LinqTestItem { Name = "Charlie", Age = 35 },
        });

        var result = await col.AsQueryable()
            .Where(x => x.Age >= 30)
            .ToListAsync();

        Assert.Equal(2, result.Count);
        Assert.All(result, r => Assert.True(r.Age >= 30));
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Linq_Select_ProjectsFields()
    {
        var col = _fixture.GetCollection<LinqTestItem>("agg_linq_select");
        await col.InsertManyAsync(new[]
        {
            new LinqTestItem { Name = "Alice", Age = 30 },
            new LinqTestItem { Name = "Bob", Age = 25 },
        });

        var result = await col.AsQueryable()
            .Select(x => new { x.Name })
            .ToListAsync();

        Assert.Equal(2, result.Count);
        Assert.Contains(result, r => r.Name == "Alice");
        Assert.Contains(result, r => r.Name == "Bob");
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Linq_OrderBy_SortsDocuments()
    {
        var col = _fixture.GetCollection<LinqTestItem>("agg_linq_orderby");
        await col.InsertManyAsync(new[]
        {
            new LinqTestItem { Name = "Charlie", Age = 35 },
            new LinqTestItem { Name = "Alice", Age = 30 },
            new LinqTestItem { Name = "Bob", Age = 25 },
        });

        var result = await col.AsQueryable()
            .OrderBy(x => x.Age)
            .ToListAsync();

        Assert.Equal(3, result.Count);
        Assert.Equal("Bob", result[0].Name);
        Assert.Equal("Alice", result[1].Name);
        Assert.Equal("Charlie", result[2].Name);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Linq_Count_ReturnsDocumentCount()
    {
        var col = _fixture.GetCollection<LinqTestItem>("agg_linq_count");
        await col.InsertManyAsync(new[]
        {
            new LinqTestItem { Name = "Alice", Age = 30 },
            new LinqTestItem { Name = "Bob", Age = 25 },
        });

        var count = col.AsQueryable().Count();

        Assert.Equal(2, count);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Linq_WhereAndOrderByAndTake()
    {
        var col = _fixture.GetCollection<LinqTestItem>("agg_linq_combined");
        await col.InsertManyAsync(new[]
        {
            new LinqTestItem { Name = "A", Age = 50 },
            new LinqTestItem { Name = "B", Age = 40 },
            new LinqTestItem { Name = "C", Age = 30 },
            new LinqTestItem { Name = "D", Age = 20 },
            new LinqTestItem { Name = "E", Age = 10 },
        });

        var result = await col.AsQueryable()
            .Where(x => x.Age >= 30)
            .OrderByDescending(x => x.Age)
            .Take(2)
            .ToListAsync();

        Assert.Equal(2, result.Count);
        Assert.Equal("A", result[0].Name);
        Assert.Equal("B", result[1].Name);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Linq_Any_ReturnsTrueWhenMatching()
    {
        var col = _fixture.GetCollection<LinqTestItem>("agg_linq_any");
        await col.InsertManyAsync(new[]
        {
            new LinqTestItem { Name = "Alice", Age = 30 },
            new LinqTestItem { Name = "Bob", Age = 25 },
        });

        var anyOver40 = col.AsQueryable().Any(x => x.Age > 40);
        var anyOver20 = col.AsQueryable().Any(x => x.Age > 20);

        Assert.False(anyOver40);
        Assert.True(anyOver20);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Linq_First_ReturnsSingleDocument()
    {
        var col = _fixture.GetCollection<LinqTestItem>("agg_linq_first");
        await col.InsertManyAsync(new[]
        {
            new LinqTestItem { Name = "Bob", Age = 25 },
            new LinqTestItem { Name = "Alice", Age = 30 },
        });

        var first = col.AsQueryable()
            .OrderBy(x => x.Name)
            .First();

        Assert.Equal("Alice", first.Name);
    }

    /// <summary>Simple POCO for LINQ tests.</summary>
    private class LinqTestItem
    {
        [MongoDB.Bson.Serialization.Attributes.BsonId]
        [MongoDB.Bson.Serialization.Attributes.BsonRepresentation(BsonType.ObjectId)]
        public string? Id { get; set; }
        public string Name { get; set; } = "";
        public int Age { get; set; }
    }

    #endregion

    #region Fluent API terminal methods

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Fluent_FirstAsync_ReturnsSingleDocument()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_fluent_first");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "name", "Alice" }, { "score", 90 } },
            new BsonDocument { { "name", "Bob" }, { "score", 80 } },
        });

        var result = await col.Aggregate()
            .SortByDescending(d => d["score"])
            .FirstAsync();

        Assert.Equal("Alice", result["name"].AsString);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Fluent_SingleAsync_ThrowsWhenMultiple()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_fluent_single");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "v", 1 } },
            new BsonDocument { { "v", 2 } },
        });

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            col.Aggregate().SingleAsync());
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Fluent_AnyAsync_ChecksExistence()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_fluent_any");
        await col.InsertOneAsync(new BsonDocument { { "v", 1 } });

        var anyAll = await col.Aggregate().AnyAsync();
        var anyMatch = await col.Aggregate()
            .Match(Builders<BsonDocument>.Filter.Eq("v", 99))
            .AnyAsync();

        Assert.True(anyAll);
        Assert.False(anyMatch);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Fluent_ToCursorAsync_ReturnsValidCursor()
    {
        var col = _fixture.GetCollection<BsonDocument>("agg_fluent_cursor");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "v", 1 } },
            new BsonDocument { { "v", 2 } },
        });

        using var cursor = await col.Aggregate()
            .SortBy(d => d["v"])
            .ToCursorAsync();

        var items = new List<BsonDocument>();
        while (await cursor.MoveNextAsync())
            items.AddRange(cursor.Current);

        Assert.Equal(2, items.Count);
    }

    #endregion

    #region $densify / $fill / $documents

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task Documents_CreatesDocumentsFromStage()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/documents/
        // $documents must be used in a database-level pipeline or as first stage
        var db = _fixture.Database;

        var result = await db.Aggregate()
            .AppendStage<BsonDocument>(new BsonDocument("$documents", new BsonArray
            {
                new BsonDocument { { "x", 1 } },
                new BsonDocument { { "x", 2 } },
                new BsonDocument { { "x", 3 } },
            }))
            .ToListAsync();

        Assert.Equal(3, result.Count);
        Assert.Equal(1, result[0]["x"].ToInt32());
        Assert.Equal(2, result[1]["x"].ToInt32());
        Assert.Equal(3, result[2]["x"].ToInt32());
    }

    #endregion
}
