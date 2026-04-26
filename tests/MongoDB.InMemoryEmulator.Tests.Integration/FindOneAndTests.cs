using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoDB.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for FindOneAndDelete, FindOneAndReplace, FindOneAndUpdate.
/// </summary>
[Collection("Integration")]
public class FindOneAndTests : IAsyncLifetime
{
    private readonly MongoDbSession _session;
    private ITestCollectionFixture _fixture = null!;

    public FindOneAndTests(MongoDbSession session) => _session = session;

    public async ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        await _fixture.ResetAsync();
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    #region FindOneAndDelete

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task FindOneAndDelete_ReturnsDeletedDocument()
    {
        var col = _fixture.GetCollection<TestDoc>("findoneand_1");
        await col.InsertOneAsync(new TestDoc { Name = "Alice", Value = 1 });

        var deleted = await col.FindOneAndDeleteAsync(
            Builders<TestDoc>.Filter.Eq(x => x.Name, "Alice"));

        Assert.NotNull(deleted);
        Assert.Equal("Alice", deleted.Name);
        Assert.Equal(0, await col.CountDocumentsAsync(Builders<TestDoc>.Filter.Empty));
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task FindOneAndDelete_NoMatch_ReturnsNull()
    {
        var col = _fixture.GetCollection<TestDoc>("findoneand_1");

        var deleted = await col.FindOneAndDeleteAsync(
            Builders<TestDoc>.Filter.Eq(x => x.Name, "NonExistent"));

        Assert.Null(deleted);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task FindOneAndDelete_WithSort_DeletesFirst()
    {
        var col = _fixture.GetCollection<TestDoc>("findoneand_1");
        await col.InsertManyAsync(new[]
        {
            new TestDoc { Name = "A", Value = 3 },
            new TestDoc { Name = "A", Value = 1 },
            new TestDoc { Name = "A", Value = 2 }
        });

        var deleted = await col.FindOneAndDeleteAsync(
            Builders<TestDoc>.Filter.Eq(x => x.Name, "A"),
            new FindOneAndDeleteOptions<TestDoc>
            {
                Sort = Builders<TestDoc>.Sort.Ascending(x => x.Value)
            });

        Assert.Equal(1, deleted.Value);
        Assert.Equal(2, await col.CountDocumentsAsync(Builders<TestDoc>.Filter.Empty));
    }

    #endregion

    #region FindOneAndReplace

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task FindOneAndReplace_ReturnsBefore_ByDefault()
    {
        var col = _fixture.GetCollection<TestDoc>("findoneand_1");
        var original = new TestDoc { Name = "Alice", Value = 1 };
        await col.InsertOneAsync(original);

        var before = await col.FindOneAndReplaceAsync(
            Builders<TestDoc>.Filter.Eq(x => x.Name, "Alice"),
            new TestDoc { Id = original.Id, Name = "Alice", Value = 99 });

        Assert.Equal(1, before.Value);

        var current = await col.Find(Builders<TestDoc>.Filter.Eq(x => x.Name, "Alice")).FirstOrDefaultAsync();
        Assert.Equal(99, current.Value);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task FindOneAndReplace_ReturnAfter()
    {
        var col = _fixture.GetCollection<TestDoc>("findoneand_1");
        var original = new TestDoc { Name = "Alice", Value = 1 };
        await col.InsertOneAsync(original);

        var after = await col.FindOneAndReplaceAsync(
            Builders<TestDoc>.Filter.Eq(x => x.Name, "Alice"),
            new TestDoc { Id = original.Id, Name = "Alice", Value = 99 },
            new FindOneAndReplaceOptions<TestDoc> { ReturnDocument = ReturnDocument.After });

        Assert.Equal(99, after.Value);
    }

    #endregion

    #region FindOneAndUpdate

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task FindOneAndUpdate_ReturnsBefore_ByDefault()
    {
        var col = _fixture.GetCollection<TestDoc>("findoneand_1");
        await col.InsertOneAsync(new TestDoc { Name = "Alice", Value = 1 });

        var before = await col.FindOneAndUpdateAsync(
            Builders<TestDoc>.Filter.Eq(x => x.Name, "Alice"),
            Builders<TestDoc>.Update.Set(x => x.Value, 99));

        Assert.Equal(1, before.Value);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task FindOneAndUpdate_ReturnAfter()
    {
        var col = _fixture.GetCollection<TestDoc>("findoneand_1");
        await col.InsertOneAsync(new TestDoc { Name = "Alice", Value = 1 });

        var after = await col.FindOneAndUpdateAsync(
            Builders<TestDoc>.Filter.Eq(x => x.Name, "Alice"),
            Builders<TestDoc>.Update.Set(x => x.Value, 99),
            new FindOneAndUpdateOptions<TestDoc> { ReturnDocument = ReturnDocument.After });

        Assert.Equal(99, after.Value);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task FindOneAndUpdate_Upsert_ReturnAfter()
    {
        var col = _fixture.GetCollection<BsonDocument>("findoneand_1");

        var result = await col.FindOneAndUpdateAsync(
            Builders<BsonDocument>.Filter.Eq("name", "NewDoc"),
            Builders<BsonDocument>.Update.Set("value", 42),
            new FindOneAndUpdateOptions<BsonDocument>
            {
                IsUpsert = true,
                ReturnDocument = ReturnDocument.After
            });

        Assert.NotNull(result);
        Assert.Equal(42, result["value"].AsInt32);
        Assert.Equal("NewDoc", result["name"].AsString);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public async Task FindOneAndUpdate_WithSort_UpdatesFirst()
    {
        var col = _fixture.GetCollection<TestDoc>("findoneand_1");
        await col.InsertManyAsync(new[]
        {
            new TestDoc { Name = "A", Value = 3 },
            new TestDoc { Name = "A", Value = 1 },
            new TestDoc { Name = "A", Value = 2 }
        });

        var updated = await col.FindOneAndUpdateAsync(
            Builders<TestDoc>.Filter.Eq(x => x.Name, "A"),
            Builders<TestDoc>.Update.Set(x => x.Value, 99),
            new FindOneAndUpdateOptions<TestDoc>
            {
                Sort = Builders<TestDoc>.Sort.Ascending(x => x.Value),
                ReturnDocument = ReturnDocument.After
            });

        Assert.Equal(99, updated.Value);

        // The one with Value=1 should have been updated
        var remaining = await col.Find(Builders<TestDoc>.Filter.Eq(x => x.Name, "A"))
            .Sort(Builders<TestDoc>.Sort.Ascending(x => x.Value))
            .ToListAsync();

        Assert.Equal(2, remaining[0].Value);
        Assert.Equal(3, remaining[1].Value);
        Assert.Equal(99, remaining[2].Value);
    }

    #endregion
}
