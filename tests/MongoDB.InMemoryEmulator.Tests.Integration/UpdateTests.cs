using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoDB.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for UpdateOne and UpdateMany operations.
/// </summary>
[Collection("Integration")]
public class UpdateTests : IAsyncLifetime
{
    private readonly MongoDbSession _session;
    private ITestCollectionFixture _fixture = null!;

    public UpdateTests(MongoDbSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        await _fixture.ResetAsync();
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    #region UpdateOne - $set

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task UpdateOne_Set_UpdatesSingleDocument()
    {
        var col = _fixture.GetCollection<TestDoc>("update_1");
        await col.InsertOneAsync(new TestDoc { Name = "Alice", Value = 1 });
        await col.InsertOneAsync(new TestDoc { Name = "Bob", Value = 2 });

        var result = await col.UpdateOneAsync(
            Builders<TestDoc>.Filter.Eq(x => x.Name, "Alice"),
            Builders<TestDoc>.Update.Set(x => x.Value, 99));

        Assert.Equal(1, result.MatchedCount);
        Assert.Equal(1, result.ModifiedCount);
        Assert.True(result.IsAcknowledged);

        var alice = await col.Find(Builders<TestDoc>.Filter.Eq(x => x.Name, "Alice")).FirstOrDefaultAsync();
        Assert.Equal(99, alice.Value);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task UpdateOne_Set_NoMatch_ReturnsZero()
    {
        var col = _fixture.GetCollection<TestDoc>("update_1");
        await col.InsertOneAsync(new TestDoc { Name = "Alice", Value = 1 });

        var result = await col.UpdateOneAsync(
            Builders<TestDoc>.Filter.Eq(x => x.Name, "NonExistent"),
            Builders<TestDoc>.Update.Set(x => x.Value, 99));

        Assert.Equal(0, result.MatchedCount);
        Assert.Equal(0, result.ModifiedCount);
    }

    #endregion

    #region UpdateOne - $inc

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task UpdateOne_Inc_IncrementsValue()
    {
        var col = _fixture.GetCollection<TestDoc>("update_1");
        await col.InsertOneAsync(new TestDoc { Name = "Counter", Value = 10 });

        await col.UpdateOneAsync(
            Builders<TestDoc>.Filter.Eq(x => x.Name, "Counter"),
            Builders<TestDoc>.Update.Inc(x => x.Value, 5));

        var doc = await col.Find(Builders<TestDoc>.Filter.Eq(x => x.Name, "Counter")).FirstOrDefaultAsync();
        Assert.Equal(15, doc.Value);
    }

    #endregion

    #region UpdateOne - $unset

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task UpdateOne_Unset_RemovesField()
    {
        var col = _fixture.GetCollection<BsonDocument>("update_1");
        await col.InsertOneAsync(new BsonDocument { { "name", "Alice" }, { "extra", "data" } });

        await col.UpdateOneAsync(
            Builders<BsonDocument>.Filter.Eq("name", "Alice"),
            Builders<BsonDocument>.Update.Unset("extra"));

        var doc = await col.Find(Builders<BsonDocument>.Filter.Eq("name", "Alice")).FirstOrDefaultAsync();
        Assert.False(doc.Contains("extra"));
    }

    #endregion

    #region UpdateOne - $min / $max

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task UpdateOne_Min_UpdatesWhenSmaller()
    {
        var col = _fixture.GetCollection<TestDoc>("update_1");
        await col.InsertOneAsync(new TestDoc { Name = "A", Value = 10 });

        await col.UpdateOneAsync(
            Builders<TestDoc>.Filter.Eq(x => x.Name, "A"),
            Builders<TestDoc>.Update.Min(x => x.Value, 5));

        var doc = await col.Find(Builders<TestDoc>.Filter.Eq(x => x.Name, "A")).FirstOrDefaultAsync();
        Assert.Equal(5, doc.Value);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task UpdateOne_Max_UpdatesWhenLarger()
    {
        var col = _fixture.GetCollection<TestDoc>("update_1");
        await col.InsertOneAsync(new TestDoc { Name = "A", Value = 10 });

        await col.UpdateOneAsync(
            Builders<TestDoc>.Filter.Eq(x => x.Name, "A"),
            Builders<TestDoc>.Update.Max(x => x.Value, 20));

        var doc = await col.Find(Builders<TestDoc>.Filter.Eq(x => x.Name, "A")).FirstOrDefaultAsync();
        Assert.Equal(20, doc.Value);
    }

    #endregion

    #region UpdateOne - $mul

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task UpdateOne_Mul_MultipliesValue()
    {
        var col = _fixture.GetCollection<TestDoc>("update_1");
        await col.InsertOneAsync(new TestDoc { Name = "A", Value = 10 });

        await col.UpdateOneAsync(
            Builders<TestDoc>.Filter.Eq(x => x.Name, "A"),
            Builders<TestDoc>.Update.Mul(x => x.Value, 3));

        var doc = await col.Find(Builders<TestDoc>.Filter.Eq(x => x.Name, "A")).FirstOrDefaultAsync();
        Assert.Equal(30, doc.Value);
    }

    #endregion

    #region UpdateOne - $rename

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task UpdateOne_Rename_RenamesField()
    {
        var col = _fixture.GetCollection<BsonDocument>("update_1");
        await col.InsertOneAsync(new BsonDocument { { "oldName", "value" } });

        await col.UpdateOneAsync(
            Builders<BsonDocument>.Filter.Empty,
            Builders<BsonDocument>.Update.Rename("oldName", "newName"));

        var doc = await col.Find(Builders<BsonDocument>.Filter.Empty).FirstOrDefaultAsync();
        Assert.False(doc.Contains("oldName"));
        Assert.True(doc.Contains("newName"));
        Assert.Equal("value", doc["newName"].AsString);
    }

    #endregion

    #region UpdateOne - $currentDate

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task UpdateOne_CurrentDate_SetsDateField()
    {
        var col = _fixture.GetCollection<BsonDocument>("update_1");
        await col.InsertOneAsync(new BsonDocument { { "name", "A" } });

        await col.UpdateOneAsync(
            Builders<BsonDocument>.Filter.Eq("name", "A"),
            Builders<BsonDocument>.Update.CurrentDate("lastModified"));

        var doc = await col.Find(Builders<BsonDocument>.Filter.Eq("name", "A")).FirstOrDefaultAsync();
        Assert.True(doc.Contains("lastModified"));
        Assert.Equal(BsonType.DateTime, doc["lastModified"].BsonType);
    }

    #endregion

    #region UpdateMany

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task UpdateMany_UpdatesAllMatching()
    {
        var col = _fixture.GetCollection<TestDoc>("update_1");
        await col.InsertManyAsync(new[]
        {
            new TestDoc { Name = "A", Value = 1 },
            new TestDoc { Name = "A", Value = 2 },
            new TestDoc { Name = "B", Value = 3 }
        });

        var result = await col.UpdateManyAsync(
            Builders<TestDoc>.Filter.Eq(x => x.Name, "A"),
            Builders<TestDoc>.Update.Set(x => x.Value, 99));

        Assert.Equal(2, result.MatchedCount);
        Assert.Equal(2, result.ModifiedCount);

        var count = await col.CountDocumentsAsync(Builders<TestDoc>.Filter.Eq(x => x.Value, 99));
        Assert.Equal(2, count);
    }

    #endregion

    #region Upsert

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task UpdateOne_Upsert_InsertsWhenNoMatch()
    {
        var col = _fixture.GetCollection<BsonDocument>("update_1");

        var result = await col.UpdateOneAsync(
            Builders<BsonDocument>.Filter.Eq("name", "NewDoc"),
            Builders<BsonDocument>.Update.Set("value", 42),
            new UpdateOptions { IsUpsert = true });

        Assert.Equal(0, result.MatchedCount);
        Assert.NotNull(result.UpsertedId);

        var doc = await col.Find(Builders<BsonDocument>.Filter.Eq("name", "NewDoc")).FirstOrDefaultAsync();
        Assert.NotNull(doc);
        Assert.Equal(42, doc["value"].AsInt32);
    }

    #endregion

    #region $push / $pull / $addToSet / $pop

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task UpdateOne_Push_AddsToArray()
    {
        var col = _fixture.GetCollection<BsonDocument>("update_1");
        await col.InsertOneAsync(new BsonDocument { { "tags", new BsonArray { "a", "b" } } });

        await col.UpdateOneAsync(
            Builders<BsonDocument>.Filter.Empty,
            Builders<BsonDocument>.Update.Push("tags", "c"));

        var doc = await col.Find(Builders<BsonDocument>.Filter.Empty).FirstOrDefaultAsync();
        Assert.Equal(3, doc["tags"].AsBsonArray.Count);
        Assert.Equal("c", doc["tags"].AsBsonArray[2].AsString);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task UpdateOne_Pull_RemovesFromArray()
    {
        var col = _fixture.GetCollection<BsonDocument>("update_1");
        await col.InsertOneAsync(new BsonDocument { { "tags", new BsonArray { "a", "b", "c" } } });

        await col.UpdateOneAsync(
            Builders<BsonDocument>.Filter.Empty,
            Builders<BsonDocument>.Update.Pull("tags", "b"));

        var doc = await col.Find(Builders<BsonDocument>.Filter.Empty).FirstOrDefaultAsync();
        var tags = doc["tags"].AsBsonArray;
        Assert.Equal(2, tags.Count);
        Assert.DoesNotContain(new BsonString("b"), tags);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task UpdateOne_AddToSet_NoDuplicates()
    {
        var col = _fixture.GetCollection<BsonDocument>("update_1");
        await col.InsertOneAsync(new BsonDocument { { "tags", new BsonArray { "a", "b" } } });

        await col.UpdateOneAsync(
            Builders<BsonDocument>.Filter.Empty,
            Builders<BsonDocument>.Update.AddToSet("tags", "b"));

        var doc = await col.Find(Builders<BsonDocument>.Filter.Empty).FirstOrDefaultAsync();
        Assert.Equal(2, doc["tags"].AsBsonArray.Count);

        await col.UpdateOneAsync(
            Builders<BsonDocument>.Filter.Empty,
            Builders<BsonDocument>.Update.AddToSet("tags", "c"));

        doc = await col.Find(Builders<BsonDocument>.Filter.Empty).FirstOrDefaultAsync();
        Assert.Equal(3, doc["tags"].AsBsonArray.Count);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task UpdateOne_Pop_RemovesLastElement()
    {
        var col = _fixture.GetCollection<BsonDocument>("update_1");
        await col.InsertOneAsync(new BsonDocument { { "items", new BsonArray { 1, 2, 3 } } });

        await col.UpdateOneAsync(
            Builders<BsonDocument>.Filter.Empty,
            Builders<BsonDocument>.Update.PopLast("items"));

        var doc = await col.Find(Builders<BsonDocument>.Filter.Empty).FirstOrDefaultAsync();
        var items = doc["items"].AsBsonArray;
        Assert.Equal(2, items.Count);
        Assert.Equal(2, items[1].AsInt32);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task UpdateOne_PopFirst_RemovesFirstElement()
    {
        var col = _fixture.GetCollection<BsonDocument>("update_1");
        await col.InsertOneAsync(new BsonDocument { { "items", new BsonArray { 1, 2, 3 } } });

        await col.UpdateOneAsync(
            Builders<BsonDocument>.Filter.Empty,
            Builders<BsonDocument>.Update.PopFirst("items"));

        var doc = await col.Find(Builders<BsonDocument>.Filter.Empty).FirstOrDefaultAsync();
        var items = doc["items"].AsBsonArray;
        Assert.Equal(2, items.Count);
        Assert.Equal(2, items[0].AsInt32);
    }

    #endregion

    #region _id immutability

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task UpdateOne_SetId_ThrowsImmutableField()
    {
        var col = _fixture.GetCollection<BsonDocument>("update_1");
        await col.InsertOneAsync(new BsonDocument { { "name", "A" } });

        // Real MongoDB throws MongoWriteException; in-memory throws MongoCommandException.
        // Both contain error code 66 (ImmutableField).
        var ex = await Assert.ThrowsAnyAsync<MongoServerException>(async () =>
            await col.UpdateOneAsync(
                Builders<BsonDocument>.Filter.Eq("name", "A"),
                Builders<BsonDocument>.Update.Set("_id", "newId")));
        Assert.Contains("immutable field '_id'", ex.Message);
    }

    #endregion

    #region Combined update operators

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task UpdateOne_CombinedOperators()
    {
        var col = _fixture.GetCollection<BsonDocument>("update_1");
        await col.InsertOneAsync(new BsonDocument { { "name", "A" }, { "count", 1 }, { "score", 10 } });

        await col.UpdateOneAsync(
            Builders<BsonDocument>.Filter.Eq("name", "A"),
            Builders<BsonDocument>.Update
                .Inc("count", 1)
                .Set("score", 20));

        var doc = await col.Find(Builders<BsonDocument>.Filter.Eq("name", "A")).FirstOrDefaultAsync();
        Assert.Equal(2, doc["count"].AsInt32);
        Assert.Equal(20, doc["score"].AsInt32);
    }

    #endregion

    #region Pipeline Updates

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task UpdateOne_Pipeline_SetComputedField()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/method/db.collection.updateOne/
        //   "Starting in MongoDB 4.2, the method can accept an aggregation pipeline."
        var col = _fixture.GetCollection<BsonDocument>("upd_pipeline_set");
        await col.InsertOneAsync(new BsonDocument { { "price", 100 }, { "tax", 0.1 } });

        var pipeline = new BsonDocumentStagePipelineDefinition<BsonDocument, BsonDocument>(new[]
        {
            new BsonDocument("$set", new BsonDocument("total",
                new BsonDocument("$add", new BsonArray { "$price", new BsonDocument("$multiply", new BsonArray { "$price", "$tax" }) }))),
        });

        await col.UpdateOneAsync(
            Builders<BsonDocument>.Filter.Empty,
            Builders<BsonDocument>.Update.Pipeline(pipeline));

        var result = await col.Find(FilterDefinition<BsonDocument>.Empty).FirstOrDefaultAsync();
        Assert.Equal(110.0, result["total"].ToDouble());
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task UpdateMany_Pipeline_AddsFieldToAll()
    {
        var col = _fixture.GetCollection<BsonDocument>("upd_pipeline_many");
        await col.InsertManyAsync(new[]
        {
            new BsonDocument { { "name", "A" }, { "score", 80 } },
            new BsonDocument { { "name", "B" }, { "score", 90 } },
        });

        var pipeline = new BsonDocumentStagePipelineDefinition<BsonDocument, BsonDocument>(new[]
        {
            new BsonDocument("$set", new BsonDocument("grade",
                new BsonDocument("$cond", new BsonDocument
                {
                    { "if", new BsonDocument("$gte", new BsonArray { "$score", 85 }) },
                    { "then", "A" },
                    { "else", "B" }
                }))),
        });

        await col.UpdateManyAsync(
            Builders<BsonDocument>.Filter.Empty,
            Builders<BsonDocument>.Update.Pipeline(pipeline));

        var results = await col.Find(FilterDefinition<BsonDocument>.Empty)
            .SortBy(d => d["name"]).ToListAsync();

        Assert.Equal("B", results[0]["grade"].AsString); // score 80 < 85
        Assert.Equal("A", results[1]["grade"].AsString); // score 90 >= 85
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task UpdateOne_Pipeline_UnsetField()
    {
        var col = _fixture.GetCollection<BsonDocument>("upd_pipeline_unset");
        await col.InsertOneAsync(new BsonDocument { { "name", "test" }, { "temp", "remove_me" } });

        var pipeline = new BsonDocumentStagePipelineDefinition<BsonDocument, BsonDocument>(new[]
        {
            new BsonDocument("$unset", "temp"),
        });

        await col.UpdateOneAsync(
            Builders<BsonDocument>.Filter.Empty,
            Builders<BsonDocument>.Update.Pipeline(pipeline));

        var result = await col.Find(FilterDefinition<BsonDocument>.Empty).FirstOrDefaultAsync();
        Assert.Equal("test", result["name"].AsString);
        Assert.False(result.Contains("temp"));
    }

    #endregion
}
