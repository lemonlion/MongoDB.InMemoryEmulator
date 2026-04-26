using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;
using AwesomeAssertions;

namespace MongoDB.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Phase 4 integration tests: Extended $setWindowFields tests — statistical operators, gap filling, and more.
/// </summary>
[Collection("Integration")]
public class SetWindowFieldsTests : IAsyncLifetime
{
    private readonly MongoDbSession _session;
    private ITestCollectionFixture _fixture = null!;

    public SetWindowFieldsTests(MongoDbSession session)
    {
        _session = session;
    }

    public ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await _fixture.DisposeAsync();
    }

    #region Exponential Moving Average

    [Fact]
    public async Task SetWindowFields_ExpMovingAvg_with_N()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/expMovingAvg/
        //   "Returns the exponential moving average of numeric expressions."
        var collection = _fixture.GetCollection<BsonDocument>("swf_ema");
        await collection.InsertManyAsync(new[]
        {
            new BsonDocument { { "day", 1 }, { "price", 10.0 } },
            new BsonDocument { { "day", 2 }, { "price", 20.0 } },
            new BsonDocument { { "day", 3 }, { "price", 30.0 } },
            new BsonDocument { { "day", 4 }, { "price", 40.0 } },
        });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$setWindowFields", new BsonDocument
            {
                { "sortBy", new BsonDocument("day", 1) },
                { "output", new BsonDocument("ema", new BsonDocument("$expMovingAvg", new BsonDocument
                    {
                        { "input", "$price" },
                        { "N", 3 }
                    }))
                }
            }));

        var results = await collection.Aggregate(pipeline).ToListAsync();
        Assert.Equal(4, results.Count);

        // First value: EMA = price itself = 10.0
        Assert.Equal(10.0, results[0]["ema"].ToDouble(), 0.01);
        // Subsequent values use alpha = 2/(3+1) = 0.5
        // EMA2 = 0.5*20 + 0.5*10 = 15
        Assert.Equal(15.0, results[1]["ema"].ToDouble(), 0.01);
    }

    [Fact]
    public async Task SetWindowFields_ExpMovingAvg_with_alpha()
    {
        var collection = _fixture.GetCollection<BsonDocument>("swf_ema_alpha");
        await collection.InsertManyAsync(new[]
        {
            new BsonDocument { { "day", 1 }, { "val", 100.0 } },
            new BsonDocument { { "day", 2 }, { "val", 200.0 } },
        });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$setWindowFields", new BsonDocument
            {
                { "sortBy", new BsonDocument("day", 1) },
                { "output", new BsonDocument("ema", new BsonDocument("$expMovingAvg", new BsonDocument
                    {
                        { "input", "$val" },
                        { "alpha", 0.5 }
                    }))
                }
            }));

        var results = await collection.Aggregate(pipeline).ToListAsync();
        Assert.Equal(100.0, results[0]["ema"].ToDouble(), 0.01);
        // EMA2 = 0.5*200 + 0.5*100 = 150
        Assert.Equal(150.0, results[1]["ema"].ToDouble(), 0.01);
    }

    #endregion

    #region Derivative

    [Fact]
    public async Task SetWindowFields_Derivative()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/derivative/
        var collection = _fixture.GetCollection<BsonDocument>("swf_deriv");
        await collection.InsertManyAsync(new[]
        {
            new BsonDocument { { "t", 1 }, { "val", 10.0 } },
            new BsonDocument { { "t", 2 }, { "val", 30.0 } },
            new BsonDocument { { "t", 3 }, { "val", 25.0 } },
        });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$setWindowFields", new BsonDocument
            {
                { "sortBy", new BsonDocument("t", 1) },
                { "output", new BsonDocument("rate", new BsonDocument
                    {
                        { "$derivative", new BsonDocument("input", "$val") },
                        { "window", new BsonDocument("documents", new BsonArray { -1, 0 }) }
                    })
                }
            }));

        var results = await collection.Aggregate(pipeline).ToListAsync();
        Assert.Equal(BsonNull.Value, results[0]["rate"]); // First has no previous
        Assert.Equal(20.0, results[1]["rate"].ToDouble(), 0.01); // 30 - 10
        Assert.Equal(-5.0, results[2]["rate"].ToDouble(), 0.01); // 25 - 30
    }

    #endregion

    #region StdDev

    [Fact]
    public async Task SetWindowFields_StdDevPop()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/stdDevPop/
        var collection = _fixture.GetCollection<BsonDocument>("swf_stdpop");
        await collection.InsertManyAsync(new[]
        {
            new BsonDocument { { "idx", 1 }, { "val", 2.0 } },
            new BsonDocument { { "idx", 2 }, { "val", 4.0 } },
            new BsonDocument { { "idx", 3 }, { "val", 4.0 } },
            new BsonDocument { { "idx", 4 }, { "val", 4.0 } },
            new BsonDocument { { "idx", 5 }, { "val", 5.0 } },
            new BsonDocument { { "idx", 6 }, { "val", 5.0 } },
            new BsonDocument { { "idx", 7 }, { "val", 7.0 } },
            new BsonDocument { { "idx", 8 }, { "val", 9.0 } },
        });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$setWindowFields", new BsonDocument
            {
                { "sortBy", new BsonDocument("idx", 1) },
                { "output", new BsonDocument("stddev", new BsonDocument
                    {
                        { "$stdDevPop", "$val" },
                        { "window", new BsonDocument("documents", new BsonArray { "unbounded", "unbounded" }) }
                    })
                }
            }));

        var results = await collection.Aggregate(pipeline).ToListAsync();
        // Mean = 5, variance = 4, stddev = 2
        Assert.Equal(2.0, results[0]["stddev"].ToDouble(), 0.01);
    }

    [Fact]
    public async Task SetWindowFields_StdDevSamp()
    {
        var collection = _fixture.GetCollection<BsonDocument>("swf_stdsamp");
        await collection.InsertManyAsync(new[]
        {
            new BsonDocument { { "idx", 1 }, { "val", 10.0 } },
            new BsonDocument { { "idx", 2 }, { "val", 20.0 } },
            new BsonDocument { { "idx", 3 }, { "val", 30.0 } },
        });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$setWindowFields", new BsonDocument
            {
                { "sortBy", new BsonDocument("idx", 1) },
                { "output", new BsonDocument("samp", new BsonDocument
                    {
                        { "$stdDevSamp", "$val" },
                        { "window", new BsonDocument("documents", new BsonArray { "unbounded", "unbounded" }) }
                    })
                }
            }));

        var results = await collection.Aggregate(pipeline).ToListAsync();
        // Sample stddev of [10,20,30]: mean=20, var=100, stddev=10
        Assert.Equal(10.0, results[0]["samp"].ToDouble(), 0.01);
    }

    #endregion

    #region Linear Fill

    [Fact]
    public async Task SetWindowFields_LinearFill()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/linearFill/
        //   "Fills null and missing fields in a window using linear interpolation."
        var collection = _fixture.GetCollection<BsonDocument>("swf_linear");
        await collection.InsertManyAsync(new[]
        {
            new BsonDocument { { "idx", 1 }, { "val", 10.0 } },
            new BsonDocument { { "idx", 2 } }, // missing val
            new BsonDocument { { "idx", 3 }, { "val", 30.0 } },
        });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$setWindowFields", new BsonDocument
            {
                { "sortBy", new BsonDocument("idx", 1) },
                { "output", new BsonDocument("filled", new BsonDocument("$linearFill", "$val")) }
            }));

        var results = await collection.Aggregate(pipeline).ToListAsync();
        Assert.Equal(10.0, results[0]["filled"].ToDouble(), 0.01);
        Assert.Equal(20.0, results[1]["filled"].ToDouble(), 0.01); // Interpolated
        Assert.Equal(30.0, results[2]["filled"].ToDouble(), 0.01);
    }

    #endregion

    #region LOCF (Last Observation Carried Forward)

    [Fact]
    public async Task SetWindowFields_Locf()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/locf/
        //   "Sets values for null and missing fields to the last non-null value."
        var collection = _fixture.GetCollection<BsonDocument>("swf_locf");
        await collection.InsertManyAsync(new[]
        {
            new BsonDocument { { "idx", 1 }, { "temp", 72.0 } },
            new BsonDocument { { "idx", 2 } }, // missing temp
            new BsonDocument { { "idx", 3 } }, // missing temp
            new BsonDocument { { "idx", 4 }, { "temp", 75.0 } },
        });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$setWindowFields", new BsonDocument
            {
                { "sortBy", new BsonDocument("idx", 1) },
                { "output", new BsonDocument("filled", new BsonDocument("$locf", "$temp")) }
            }));

        var results = await collection.Aggregate(pipeline).ToListAsync();
        Assert.Equal(72.0, results[0]["filled"].ToDouble(), 0.01);
        Assert.Equal(72.0, results[1]["filled"].ToDouble(), 0.01); // Carried forward
        Assert.Equal(72.0, results[2]["filled"].ToDouble(), 0.01); // Carried forward
        Assert.Equal(75.0, results[3]["filled"].ToDouble(), 0.01);
    }

    #endregion

    #region Covariance

    [Fact]
    public async Task SetWindowFields_CovariancePop()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/covariancePop/
        var collection = _fixture.GetCollection<BsonDocument>("swf_covpop");
        await collection.InsertManyAsync(new[]
        {
            new BsonDocument { { "idx", 1 }, { "x", 1.0 }, { "y", 2.0 } },
            new BsonDocument { { "idx", 2 }, { "x", 2.0 }, { "y", 4.0 } },
            new BsonDocument { { "idx", 3 }, { "x", 3.0 }, { "y", 6.0 } },
        });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$setWindowFields", new BsonDocument
            {
                { "sortBy", new BsonDocument("idx", 1) },
                { "output", new BsonDocument("cov", new BsonDocument
                    {
                        { "$covariancePop", new BsonArray { "$x", "$y" } },
                        { "window", new BsonDocument("documents", new BsonArray { "unbounded", "unbounded" }) }
                    })
                }
            }));

        var results = await collection.Aggregate(pipeline).ToListAsync();
        // X = [1,2,3], Y = [2,4,6], perfect linear: cov = 2/3 * 2 = 4/3 ≈ 1.333
        // Actually: mean_x=2, mean_y=4, sum((xi-2)*(yi-4)) = (-1)(-2)+0*0+1*2 = 4, cov=4/3 ≈ 1.333
        Assert.True(results[0]["cov"].ToDouble() > 1.3);
    }

    #endregion

    #region AddToSet

    [Fact]
    public async Task SetWindowFields_AddToSet()
    {
        var collection = _fixture.GetCollection<BsonDocument>("swf_addtoset");
        await collection.InsertManyAsync(new[]
        {
            new BsonDocument { { "idx", 1 }, { "color", "red" } },
            new BsonDocument { { "idx", 2 }, { "color", "blue" } },
            new BsonDocument { { "idx", 3 }, { "color", "red" } },
        });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$setWindowFields", new BsonDocument
            {
                { "sortBy", new BsonDocument("idx", 1) },
                { "output", new BsonDocument("uniqueColors", new BsonDocument
                    {
                        { "$addToSet", "$color" },
                        { "window", new BsonDocument("documents", new BsonArray { "unbounded", "unbounded" }) }
                    })
                }
            }));

        var results = await collection.Aggregate(pipeline).ToListAsync();
        var colors = results[0]["uniqueColors"].AsBsonArray.Select(v => v.AsString).ToList();
        Assert.Equal(2, colors.Count);
        Assert.Contains("red", colors);
        Assert.Contains("blue", colors);
    }

    #endregion

    #region Document Number with Partitions

    [Fact]
    public async Task SetWindowFields_DocumentNumber_with_partition()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/documentNumber/
        var collection = _fixture.GetCollection<BsonDocument>("swf_docnum");
        await collection.InsertManyAsync(new[]
        {
            new BsonDocument { { "dept", "A" }, { "name", "Alice" }, { "score", 90 } },
            new BsonDocument { { "dept", "A" }, { "name", "Bob" }, { "score", 80 } },
            new BsonDocument { { "dept", "B" }, { "name", "Charlie" }, { "score", 70 } },
            new BsonDocument { { "dept", "B" }, { "name", "Diana" }, { "score", 95 } },
        });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$setWindowFields", new BsonDocument
            {
                { "partitionBy", "$dept" },
                { "sortBy", new BsonDocument("score", -1) },
                { "output", new BsonDocument("position", new BsonDocument("$documentNumber", new BsonDocument())) }
            }));

        var results = await collection.Aggregate(pipeline).ToListAsync();
        Assert.Equal(4, results.Count);

        // Each partition should have document numbers 1,2
        var deptA = results.Where(r => r["dept"].AsString == "A").ToList();
        Assert.Contains(deptA, r => r["position"].AsInt32 == 1);
        Assert.Contains(deptA, r => r["position"].AsInt32 == 2);
    }

    #endregion

    #region Dense Rank

    [Fact]
    public async Task SetWindowFields_DenseRank_with_ties()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/denseRank/
        var collection = _fixture.GetCollection<BsonDocument>("swf_denserank");
        await collection.InsertManyAsync(new[]
        {
            new BsonDocument { { "name", "A" }, { "score", 100 } },
            new BsonDocument { { "name", "B" }, { "score", 90 } },
            new BsonDocument { { "name", "C" }, { "score", 90 } },
            new BsonDocument { { "name", "D" }, { "score", 80 } },
        });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$setWindowFields", new BsonDocument
            {
                { "sortBy", new BsonDocument("score", -1) },
                { "output", new BsonDocument("denseRank", new BsonDocument("$denseRank", new BsonDocument())) }
            }));

        var results = await collection.Aggregate(pipeline).ToListAsync();
        var sorted = results.OrderByDescending(r => r["score"].AsInt32).ToList();

        Assert.Equal(1, sorted[0]["denseRank"].AsInt32); // score 100
        Assert.Equal(2, sorted[1]["denseRank"].AsInt32); // score 90 (tie)
        Assert.Equal(2, sorted[2]["denseRank"].AsInt32); // score 90 (tie)
        Assert.Equal(3, sorted[3]["denseRank"].AsInt32); // score 80 (no gap)
    }

    #endregion

    #region Window Boundaries

    [Fact]
    public async Task SetWindowFields_window_current_to_unbounded()
    {
        var collection = _fixture.GetCollection<BsonDocument>("swf_window_cur_unb");
        await collection.InsertManyAsync(new[]
        {
            new BsonDocument { { "idx", 1 }, { "val", 10 } },
            new BsonDocument { { "idx", 2 }, { "val", 20 } },
            new BsonDocument { { "idx", 3 }, { "val", 30 } },
        });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$setWindowFields", new BsonDocument
            {
                { "sortBy", new BsonDocument("idx", 1) },
                { "output", new BsonDocument("remaining", new BsonDocument
                    {
                        { "$sum", "$val" },
                        { "window", new BsonDocument("documents", new BsonArray { "current", "unbounded" }) }
                    })
                }
            }));

        var results = await collection.Aggregate(pipeline).ToListAsync();
        // idx=1: sum of 10+20+30 = 60
        Assert.Equal(60, results[0]["remaining"].ToInt32());
        // idx=2: sum of 20+30 = 50
        Assert.Equal(50, results[1]["remaining"].ToInt32());
        // idx=3: sum of 30 = 30
        Assert.Equal(30, results[2]["remaining"].ToInt32());
    }

    #endregion
}
