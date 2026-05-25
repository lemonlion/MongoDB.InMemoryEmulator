using global::MongoDB.Bson;
using global::MongoDB.Driver;
using Xunit;
using AwesomeAssertions;

namespace InMemoryEmulator.MongoDB.Tests.Integration;

/// <summary>
/// Phase 4 integration tests: RunCommand for common database commands.
/// </summary>
[Collection("Integration")]
public class RunCommandTests : IAsyncLifetime
{
    private readonly MongoDbSession _session;
    private ITestCollectionFixture _fixture = null!;

    public RunCommandTests(MongoDbSession session)
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

    [Fact]
    public void Ping_returns_ok()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/ping/
        //   "The ping command is a simple diagnostic command."
        var result = _fixture.Database.RunCommand<BsonDocument>(new BsonDocument("ping", 1));
        Assert.Equal(1, result["ok"].ToInt32());
    }

    [Fact]
    public void BuildInfo_returns_version()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/buildInfo/
        var result = _fixture.Database.RunCommand<BsonDocument>(new BsonDocument("buildInfo", 1));
        Assert.Equal(1, result["ok"].ToInt32());
        Assert.True(result.Contains("version"));
    }

    [Fact]
    public void ServerStatus_returns_host()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/serverStatus/
        var result = _fixture.Database.RunCommand<BsonDocument>(new BsonDocument("serverStatus", 1));
        Assert.Equal(1, result["ok"].ToInt32());
        Assert.True(result.Contains("host"));
        Assert.True(result.Contains("connections"));
    }

    [Fact]
    public void HostInfo_returns_system_info()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/hostInfo/
        var result = _fixture.Database.RunCommand<BsonDocument>(new BsonDocument("hostInfo", 1));
        Assert.Equal(1, result["ok"].ToInt32());
        Assert.True(result.Contains("system"));
    }

    [Fact]
    public void ConnectionStatus_returns_ok()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/connectionStatus/
        var result = _fixture.Database.RunCommand<BsonDocument>(new BsonDocument("connectionStatus", 1));
        Assert.Equal(1, result["ok"].ToInt32());
        Assert.True(result.Contains("authInfo"));
    }

    [Fact]
    public async Task Count_returns_document_count()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/count/
        var collection = _fixture.GetCollection<BsonDocument>("rc_count");
        await collection.InsertManyAsync(new[]
        {
            new BsonDocument { { "x", 1 } },
            new BsonDocument { { "x", 2 } },
            new BsonDocument { { "x", 3 } },
        });

        var result = _fixture.Database.RunCommand<BsonDocument>(
            new BsonDocument { { "count", "rc_count" } });

        Assert.Equal(1, result["ok"].ToInt32());
        Assert.Equal(3, result["n"].ToInt32());
    }

    [Fact]
    public async Task Count_with_query_filters()
    {
        var collection = _fixture.GetCollection<BsonDocument>("rc_count_q");
        await collection.InsertManyAsync(new[]
        {
            new BsonDocument { { "status", "active" } },
            new BsonDocument { { "status", "active" } },
            new BsonDocument { { "status", "inactive" } },
        });

        var result = _fixture.Database.RunCommand<BsonDocument>(new BsonDocument
        {
            { "count", "rc_count_q" },
            { "query", new BsonDocument("status", "active") }
        });

        Assert.Equal(2, result["n"].AsInt32);
    }

    [Fact]
    public async Task Distinct_returns_unique_values()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/distinct/
        var collection = _fixture.GetCollection<BsonDocument>("rc_distinct");
        await collection.InsertManyAsync(new[]
        {
            new BsonDocument { { "color", "red" } },
            new BsonDocument { { "color", "blue" } },
            new BsonDocument { { "color", "red" } },
            new BsonDocument { { "color", "green" } },
        });

        var result = _fixture.Database.RunCommand<BsonDocument>(new BsonDocument
        {
            { "distinct", "rc_distinct" },
            { "key", "color" }
        });

        Assert.Equal(1, result["ok"].ToInt32());
        var values = result["values"].AsBsonArray.Select(v => v.AsString).ToList();
        Assert.Equal(3, values.Count);
        Assert.Contains("red", values);
        Assert.Contains("blue", values);
        Assert.Contains("green", values);
    }

    [Fact]
    public async Task CollStats_returns_collection_info()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/collStats/
        var collection = _fixture.GetCollection<BsonDocument>("rc_stats");
        await collection.InsertManyAsync(new[]
        {
            new BsonDocument { { "a", 1 } },
            new BsonDocument { { "a", 2 } },
        });

        var result = _fixture.Database.RunCommand<BsonDocument>(
            new BsonDocument("collStats", "rc_stats"));

        Assert.Equal(1, result["ok"].ToInt32());
        Assert.Equal(2, result["count"].ToInt32());
    }

    [Fact]
    public void DbStats_returns_database_info()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/dbStats/
        var result = _fixture.Database.RunCommand<BsonDocument>(
            new BsonDocument("dbStats", 1));

        Assert.Equal(1, result["ok"].ToInt32());
        Assert.True(result.Contains("collections"));
        Assert.True(result.Contains("objects"));
    }

    [Fact]
    public void Create_command_creates_collection()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/create/
        var result = _fixture.Database.RunCommand<BsonDocument>(
            new BsonDocument("create", "rc_created"));

        Assert.Equal(1, result["ok"].ToInt32());

        // Verify collection exists
        var names = _fixture.Database.ListCollectionNames().ToList();
        Assert.Contains("rc_created", names);
    }

    [Fact]
    public void Drop_command_drops_collection()
    {
        _fixture.Database.CreateCollection("rc_to_drop");

        var result = _fixture.Database.RunCommand<BsonDocument>(
            new BsonDocument("drop", "rc_to_drop"));

        Assert.Equal(1, result["ok"].ToInt32());
    }

    [Fact]
    public async Task RunCommandAsync_works()
    {
        var result = await _fixture.Database.RunCommandAsync<BsonDocument>(
            new BsonDocument("ping", 1));
        Assert.Equal(1, result["ok"].ToInt32());
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public void Unknown_command_throws_MongoCommandException()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/
        //   Real MongoDB throws MongoCommandException with code 59 for unknown commands.
        var ex = Assert.Throws<MongoCommandException>(() =>
            _fixture.Database.RunCommand<BsonDocument>(
                new BsonDocument("unknownCommand", 1)));
        Assert.Contains("no such command", ex.Message);
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public void Hello_returns_writable_standalone()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/hello/
        //   "hello returns a document that describes the role of the mongod instance."
        var result = _fixture.Database.RunCommand<BsonDocument>(new BsonDocument("hello", 1));
        result["ok"].ToInt32().Should().Be(1);
        result["isWritablePrimary"].AsBoolean.Should().BeTrue();
        result.Contains("maxBsonObjectSize").Should().BeTrue();
        result.Contains("maxMessageSizeBytes").Should().BeTrue();
        result.Contains("maxWriteBatchSize").Should().BeTrue();
        result.Contains("localTime").Should().BeTrue();
        result.Contains("minWireVersion").Should().BeTrue();
        result.Contains("maxWireVersion").Should().BeTrue();
        result["readOnly"].AsBoolean.Should().BeFalse();
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public void IsMaster_returns_writable_standalone()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/isMaster/
        //   "isMaster returns a document that describes the role of the mongod instance."
        var result = _fixture.Database.RunCommand<BsonDocument>(new BsonDocument("isMaster", 1));
        result["ok"].ToInt32().Should().Be(1);
        result["ismaster"].AsBoolean.Should().BeTrue();
        result.Contains("maxBsonObjectSize").Should().BeTrue();
        result.Contains("maxWriteBatchSize").Should().BeTrue();
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.All)]
    public void Ismaster_lowercase_also_works()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/isMaster/
        //   Driver sends "ismaster" (lowercase) in some versions.
        var result = _fixture.Database.RunCommand<BsonDocument>(new BsonDocument("ismaster", 1));
        result["ok"].ToInt32().Should().Be(1);
        result["ismaster"].AsBoolean.Should().BeTrue();
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    public void CurrentOp_returns_empty_inprog()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/currentOp/
        //   "Returns a document that contains information on in-progress operations."
        var result = _fixture.Database.RunCommand<BsonDocument>(new BsonDocument("currentOp", 1));
        result["ok"].ToInt32().Should().Be(1);
        result["inprog"].AsBsonArray.Should().BeEmpty();
    }

    [Fact]
    public async Task CollMod_updates_validator()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/collMod/
        //   "collMod makes it possible to add options to a collection or to modify view definitions."
        var collName = "rc_collmod_validator";
        _fixture.Database.CreateCollection(collName);

        var validator = new BsonDocument("$jsonSchema", new BsonDocument
        {
            { "bsonType", "object" },
            { "required", new BsonArray { "name" } }
        });

        var result = _fixture.Database.RunCommand<BsonDocument>(new BsonDocument
        {
            { "collMod", collName },
            { "validator", validator },
            { "validationAction", "error" },
            { "validationLevel", "strict" }
        });
        result["ok"].ToInt32().Should().Be(1);

        // Validator should now reject docs without "name"
        var collection = _fixture.GetCollection<BsonDocument>(collName);
        await collection.InsertOneAsync(new BsonDocument("name", "Alice"));

        var ex = await Assert.ThrowsAsync<MongoWriteException>(async () =>
            await collection.InsertOneAsync(new BsonDocument("age", 30)));
        ex.Message.Should().Contain("validation");
    }

    [Fact]
    public async Task CollMod_updates_validationAction_to_warn()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/collMod/
        //   "validationAction: 'warn' logs a warning but allows the write."
        var collName = "rc_collmod_warn";

        // Create with strict validator via RunCommand
        _fixture.Database.RunCommand<BsonDocument>(new BsonDocument
        {
            { "create", collName },
            { "validator", new BsonDocument("$jsonSchema", new BsonDocument
                {
                    { "bsonType", "object" },
                    { "required", new BsonArray { "name" } }
                })
            },
            { "validationAction", "error" }
        });

        // Downgrade to warn
        _fixture.Database.RunCommand<BsonDocument>(new BsonDocument
        {
            { "collMod", collName },
            { "validationAction", "warn" }
        });

        // Should succeed now (warn instead of error)
        var collection = _fixture.GetCollection<BsonDocument>(collName);
        await collection.InsertOneAsync(new BsonDocument("age", 30));
        var count = await collection.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);
        count.Should().Be(1);
    }

    [Fact]
    public void CollMod_on_nonexistent_collection_throws()
    {
        var ex = Assert.Throws<MongoCommandException>(() =>
            _fixture.Database.RunCommand<BsonDocument>(new BsonDocument
            {
                { "collMod", "rc_does_not_exist" }
            }));
        ex.Message.Should().Contain("ns");
    }

    [Fact]
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    public void CollMod_on_nonexistent_collection_error_message_matches_mongo()
    {
        var ex = Assert.Throws<MongoCommandException>(() =>
            _fixture.Database.RunCommand<BsonDocument>(new BsonDocument
            {
                { "collMod", "rc_does_not_exist_2" }
            }));
        ex.Message.Should().Contain("ns does not exist");
    }
}
