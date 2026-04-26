using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.InMemoryEmulator.JsTriggers;
using Xunit;

namespace MongoDB.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for JavaScript expression support via Jint ($where, $function, $accumulator).
/// </summary>
public class JavaScriptExpressionTests : IDisposable
{
    public JavaScriptExpressionTests()
    {
        JsExpressionSetup.Register();
    }

    public void Dispose() { }

    private IMongoCollection<BsonDocument> CreateCollection()
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("test");
        return db.GetCollection<BsonDocument>("items");
    }

    #region $where

    [Fact]
    public void Where_SimpleExpression()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/where/
        //   "Use the $where operator to pass either a string containing a JavaScript expression
        //    or a full JavaScript function to the query system."
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "a", 10 }, { "b", 5 } });
        col.InsertOne(new BsonDocument { { "a", 3 }, { "b", 7 } });
        col.InsertOne(new BsonDocument { { "a", 20 }, { "b", 10 } });

        // Find docs where a > b
        var filter = new BsonDocument("$where", "this.a > this.b");
        var results = col.Find(filter).ToList();

        Assert.Equal(2, results.Count);
        Assert.All(results, d => Assert.True(d["a"].ToInt32() > d["b"].ToInt32()));
    }

    [Fact]
    public void Where_FunctionSyntax()
    {
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "x", 10 } });
        col.InsertOne(new BsonDocument { { "x", 25 } });
        col.InsertOne(new BsonDocument { { "x", 30 } });

        var filter = new BsonDocument("$where", "function() { return this.x > 20; }");
        var results = col.Find(filter).ToList();

        Assert.Equal(2, results.Count);
        Assert.All(results, d => Assert.True(d["x"].ToInt32() > 20));
    }

    #endregion

    #region $function

    [Fact]
    public void Function_CustomExpressionInAddFields()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/function/
        //   "Defines a custom aggregation function or expression in JavaScript."
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "x", 3 }, { "y", 4 } });
        col.InsertOne(new BsonDocument { { "x", 5 }, { "y", 12 } });

        var pipeline = new[]
        {
            new BsonDocument("$addFields", new BsonDocument("sum", new BsonDocument("$function", new BsonDocument
            {
                { "body", "function(a, b) { return a + b; }" },
                { "args", new BsonArray { "$x", "$y" } },
                { "lang", "js" }
            })))
        };

        var results = col.Aggregate<BsonDocument>(pipeline).ToList();
        Assert.Equal(2, results.Count);
        Assert.Equal(7, results[0]["sum"].ToInt32());
        Assert.Equal(17, results[1]["sum"].ToInt32());
    }

    [Fact]
    public void Function_StringManipulation()
    {
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "firstName", "John" }, { "lastName", "Doe" } });

        var pipeline = new[]
        {
            new BsonDocument("$addFields", new BsonDocument("fullName", new BsonDocument("$function", new BsonDocument
            {
                { "body", "function(first, last) { return first + ' ' + last; }" },
                { "args", new BsonArray { "$firstName", "$lastName" } },
                { "lang", "js" }
            })))
        };

        var results = col.Aggregate<BsonDocument>(pipeline).ToList();
        Assert.Single(results);
        Assert.Equal("John Doe", results[0]["fullName"].AsString);
    }

    #endregion

    #region $accumulator

    [Fact]
    public void Accumulator_CustomSumInGroup()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/accumulator/
        //   "Defines a custom accumulator function."
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "category", "A" }, { "value", 10 } });
        col.InsertOne(new BsonDocument { { "category", "A" }, { "value", 20 } });
        col.InsertOne(new BsonDocument { { "category", "B" }, { "value", 5 } });

        var pipeline = new[]
        {
            new BsonDocument("$group", new BsonDocument
            {
                { "_id", "$category" },
                { "total", new BsonDocument("$accumulator", new BsonDocument
                    {
                        { "init", "function() { return 0; }" },
                        { "accumulate", "function(state, val) { return state + val; }" },
                        { "accumulateArgs", new BsonArray { "$value" } },
                        { "merge", "function(a, b) { return a + b; }" },
                        { "lang", "js" }
                    })
                }
            })
        };

        var results = col.Aggregate<BsonDocument>(pipeline).ToList()
            .OrderBy(d => d["_id"].AsString).ToList();
        Assert.Equal(2, results.Count);
        Assert.Equal("A", results[0]["_id"].AsString);
        Assert.Equal(30, results[0]["total"].ToInt32());
        Assert.Equal("B", results[1]["_id"].AsString);
        Assert.Equal(5, results[1]["total"].ToInt32());
    }

    [Fact]
    public void Accumulator_WithFinalize()
    {
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "group", "X" }, { "val", 10 } });
        col.InsertOne(new BsonDocument { { "group", "X" }, { "val", 20 } });
        col.InsertOne(new BsonDocument { { "group", "X" }, { "val", 30 } });

        var pipeline = new[]
        {
            new BsonDocument("$group", new BsonDocument
            {
                { "_id", "$group" },
                { "avg", new BsonDocument("$accumulator", new BsonDocument
                    {
                        { "init", "function() { return { sum: 0, count: 0 }; }" },
                        { "accumulate", "function(state, val) { state.sum += val; state.count++; return state; }" },
                        { "accumulateArgs", new BsonArray { "$val" } },
                        { "merge", "function(a, b) { return { sum: a.sum + b.sum, count: a.count + b.count }; }" },
                        { "finalize", "function(state) { return state.sum / state.count; }" },
                        { "lang", "js" }
                    })
                }
            })
        };

        var results = col.Aggregate<BsonDocument>(pipeline).ToList();
        Assert.Single(results);
        Assert.Equal(20, results[0]["avg"].ToInt32());
    }

    #endregion
}
