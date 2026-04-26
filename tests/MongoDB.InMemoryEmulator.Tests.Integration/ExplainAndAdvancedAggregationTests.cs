using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoDB.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for the explain command and advanced aggregation stages.
/// </summary>
public class ExplainAndAdvancedAggregationTests
{
    private static (InMemoryMongoClient client, IMongoCollection<BsonDocument> col) Create()
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("testdb");
        return (client, db.GetCollection<BsonDocument>("items"));
    }

    [Fact]
    public void Explain_command_returns_execution_stats()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/explain/
        var (client, col) = Create();
        col.InsertOne(new BsonDocument { { "_id", 1 }, { "val", "a" } });
        col.InsertOne(new BsonDocument { { "_id", 2 }, { "val", "b" } });

        var db = client.GetDatabase("testdb");
        var result = db.RunCommand<BsonDocument>(new BsonDocumentCommand<BsonDocument>(
            new BsonDocument
            {
                { "explain", new BsonDocument { { "find", "items" }, { "filter", new BsonDocument() } } },
                { "verbosity", "executionStats" }
            }));

        Assert.Equal(1, result["ok"].AsInt32);
        Assert.True(result["executionStats"]["executionSuccess"].AsBoolean);
        Assert.Equal(2, result["executionStats"]["nReturned"].AsInt32);
        Assert.Equal("COLLSCAN", result["queryPlanner"]["winningPlan"]["stage"].AsString);
    }

    [Fact]
    public void Explain_with_empty_collection()
    {
        var (client, _) = Create();
        var db = client.GetDatabase("testdb");
        var result = db.RunCommand<BsonDocument>(new BsonDocumentCommand<BsonDocument>(
            new BsonDocument
            {
                { "explain", new BsonDocument { { "find", "empty_col" } } }
            }));

        Assert.Equal(1, result["ok"].AsInt32);
        Assert.Equal(0, result["executionStats"]["nReturned"].AsInt32);
    }

    [Fact]
    public void Facet_runs_multiple_sub_pipelines()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/facet/
        var (_, col) = Create();
        col.InsertMany(Enumerable.Range(1, 10).Select(i =>
            new BsonDocument { { "_id", i }, { "category", i % 2 == 0 ? "even" : "odd" }, { "value", i * 10 } }));

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$facet", new BsonDocument
            {
                { "categoryCounts", new BsonArray
                    {
                        new BsonDocument("$group", new BsonDocument { { "_id", "$category" }, { "count", new BsonDocument("$sum", 1) } })
                    }
                },
                { "totalValue", new BsonArray
                    {
                        new BsonDocument("$group", new BsonDocument { { "_id", BsonNull.Value }, { "total", new BsonDocument("$sum", "$value") } })
                    }
                }
            }));

        var results = col.Aggregate(pipeline).ToList();
        Assert.Single(results);
        var facetResult = results[0];

        var categoryCounts = facetResult["categoryCounts"].AsBsonArray;
        Assert.Equal(2, categoryCounts.Count);

        var totalValue = facetResult["totalValue"].AsBsonArray;
        Assert.Single(totalValue);
        Assert.Equal(550, totalValue[0]["total"].ToInt32()); // sum(10..100 step 10)
    }

    [Fact]
    public void Bucket_groups_by_boundaries()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/bucket/
        var (_, col) = Create();
        col.InsertMany(new[]
        {
            new BsonDocument { { "_id", 1 }, { "score", 10 } },
            new BsonDocument { { "_id", 2 }, { "score", 25 } },
            new BsonDocument { { "_id", 3 }, { "score", 35 } },
            new BsonDocument { { "_id", 4 }, { "score", 55 } },
            new BsonDocument { { "_id", 5 }, { "score", 75 } },
            new BsonDocument { { "_id", 6 }, { "score", 90 } }
        });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$bucket", new BsonDocument
            {
                { "groupBy", "$score" },
                { "boundaries", new BsonArray { 0, 30, 60, 100 } },
                { "output", new BsonDocument { { "count", new BsonDocument("$sum", 1) } } }
            }));

        var results = col.Aggregate(pipeline).ToList();
        Assert.Equal(3, results.Count);
        Assert.Equal(2, results[0]["count"].ToInt32()); // 10, 25
        Assert.Equal(2, results[1]["count"].ToInt32()); // 35, 55
        Assert.Equal(2, results[2]["count"].ToInt32()); // 75, 90
    }

    [Fact]
    public void BucketAuto_creates_roughly_equal_buckets()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/bucketAuto/
        var (_, col) = Create();
        col.InsertMany(Enumerable.Range(1, 12).Select(i =>
            new BsonDocument { { "_id", i }, { "value", i } }));

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$bucketAuto", new BsonDocument
            {
                { "groupBy", "$value" },
                { "buckets", 3 }
            }));

        var results = col.Aggregate(pipeline).ToList();
        Assert.Equal(3, results.Count);
        // Each bucket should have ~4 items
        foreach (var bucket in results)
        {
            Assert.True(bucket["count"].ToInt32() >= 3 && bucket["count"].ToInt32() <= 5);
        }
    }

    [Fact]
    public void GraphLookup_traverses_hierarchy()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/graphLookup/
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("testdb");
        var col = db.GetCollection<BsonDocument>("employees");

        col.InsertMany(new[]
        {
            new BsonDocument { { "_id", "CEO" }, { "name", "Alice" }, { "reportsTo", BsonNull.Value } },
            new BsonDocument { { "_id", "VP" }, { "name", "Bob" }, { "reportsTo", "CEO" } },
            new BsonDocument { { "_id", "MGR" }, { "name", "Carol" }, { "reportsTo", "VP" } },
            new BsonDocument { { "_id", "DEV" }, { "name", "Dave" }, { "reportsTo", "MGR" } }
        });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$match", new BsonDocument("_id", "DEV")),
            new BsonDocument("$graphLookup", new BsonDocument
            {
                { "from", "employees" },
                { "startWith", "$reportsTo" },
                { "connectFromField", "reportsTo" },
                { "connectToField", "_id" },
                { "as", "chain" },
                { "depthField", "depth" }
            }));

        var results = col.Aggregate(pipeline).ToList();
        Assert.Single(results);
        var chain = results[0]["chain"].AsBsonArray;
        Assert.Equal(3, chain.Count); // MGR, VP, CEO
    }

    [Fact]
    public void Merge_writes_results_to_another_collection()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/merge/
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("testdb");
        var source = db.GetCollection<BsonDocument>("source");
        var target = db.GetCollection<BsonDocument>("target");

        source.InsertMany(new[]
        {
            new BsonDocument { { "_id", 1 }, { "val", 10 } },
            new BsonDocument { { "_id", 2 }, { "val", 20 } }
        });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$merge", new BsonDocument
            {
                { "into", "target" }
            }));

        source.Aggregate(pipeline).ToList();

        var targetDocs = target.Find(FilterDefinition<BsonDocument>.Empty).ToList();
        Assert.Equal(2, targetDocs.Count);
    }

    [Fact]
    public void Out_writes_results_to_collection()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/out/
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("testdb");
        var source = db.GetCollection<BsonDocument>("source");

        source.InsertMany(new[]
        {
            new BsonDocument { { "_id", 1 }, { "val", "a" } },
            new BsonDocument { { "_id", 2 }, { "val", "b" } }
        });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$out", "output_col"));

        source.Aggregate(pipeline).ToList();

        var output = db.GetCollection<BsonDocument>("output_col");
        var docs = output.Find(FilterDefinition<BsonDocument>.Empty).ToList();
        Assert.Equal(2, docs.Count);
    }

    [Fact]
    public void Densify_fills_numeric_gaps()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/densify/
        var (_, col) = Create();
        col.InsertMany(new[]
        {
            new BsonDocument { { "_id", 1 }, { "val", 0 } },
            new BsonDocument { { "_id", 2 }, { "val", 5 } }
        });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$densify", new BsonDocument
            {
                { "field", "val" },
                { "range", new BsonDocument
                    {
                        { "step", 1 },
                        { "bounds", "full" }
                    }
                }
            }));

        var results = col.Aggregate(pipeline).ToList();
        // Should have values 0, 1, 2, 3, 4, 5
        Assert.Equal(6, results.Count);
    }

    [Fact]
    public void Fill_with_locf_fills_missing_values()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/fill/
        var (_, col) = Create();
        col.InsertMany(new[]
        {
            new BsonDocument { { "_id", 1 }, { "val", 10 } },
            new BsonDocument { { "_id", 2 } },
            new BsonDocument { { "_id", 3 } },
            new BsonDocument { { "_id", 4 }, { "val", 40 } }
        });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$sort", new BsonDocument("_id", 1)),
            new BsonDocument("$fill", new BsonDocument
            {
                { "sortBy", new BsonDocument("_id", 1) },
                { "output", new BsonDocument("val", new BsonDocument("method", "locf")) }
            }));

        var results = col.Aggregate(pipeline).ToList();
        Assert.Equal(4, results.Count);
        Assert.Equal(10, results[0]["val"].ToInt32());
        Assert.Equal(10, results[1]["val"].ToInt32()); // filled from previous
        Assert.Equal(10, results[2]["val"].ToInt32()); // carried forward
        Assert.Equal(40, results[3]["val"].ToInt32());
    }

    [Fact]
    public void Documents_creates_documents_in_pipeline()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/documents/
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("testdb");

        // Database-level aggregate with $documents
        var pipeline = PipelineDefinition<NoPipelineInput, BsonDocument>.Create(
            new BsonDocument("$documents", new BsonArray
            {
                new BsonDocument { { "x", 1 } },
                new BsonDocument { { "x", 2 } },
                new BsonDocument { { "x", 3 } }
            }));

        var results = db.Aggregate(pipeline).ToList();
        Assert.Equal(3, results.Count);
        Assert.Equal(1, results[0]["x"].AsInt32);
    }

    [Fact]
    public void Redact_with_keep_and_prune()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/redact/
        var (_, col) = Create();
        col.InsertOne(new BsonDocument
        {
            { "_id", 1 },
            { "level", 5 },
            { "data", "secret" }
        });
        col.InsertOne(new BsonDocument
        {
            { "_id", 2 },
            { "level", 1 },
            { "data", "public" }
        });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$redact", new BsonDocument("$cond", new BsonDocument
            {
                { "if", new BsonDocument("$gte", new BsonArray { "$level", 3 }) },
                { "then", "$$KEEP" },
                { "else", "$$PRUNE" }
            })));

        var results = col.Aggregate(pipeline).ToList();
        Assert.Single(results);
        Assert.Equal(1, results[0]["_id"].AsInt32);
    }

    [Fact]
    public void Sample_returns_specified_count()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/sample/
        var (_, col) = Create();
        col.InsertMany(Enumerable.Range(1, 100).Select(i =>
            new BsonDocument { { "_id", i } }));

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$sample", new BsonDocument("size", 5)));

        var results = col.Aggregate(pipeline).ToList();
        Assert.Equal(5, results.Count);
    }

    [Fact]
    public void AggregateToCollection_with_out_stage()
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("testdb");
        var source = db.GetCollection<BsonDocument>("source");

        source.InsertMany(Enumerable.Range(1, 5).Select(i =>
            new BsonDocument { { "_id", i }, { "value", i * 10 } }));

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$match", new BsonDocument("value", new BsonDocument("$gte", 30))),
            new BsonDocument("$out", "filtered"));

        source.AggregateToCollection(pipeline);

        var output = db.GetCollection<BsonDocument>("filtered");
        var docs = output.Find(FilterDefinition<BsonDocument>.Empty).ToList();
        Assert.Equal(3, docs.Count); // 30, 40, 50
    }
}
