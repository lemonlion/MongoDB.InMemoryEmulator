using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;
using AwesomeAssertions;

namespace MongoDB.InMemoryEmulator.Tests.Integration;

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
        Assert.Equal(1, result["ok"].AsInt32);
    }

    [Fact]
    public void BuildInfo_returns_version()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/buildInfo/
        var result = _fixture.Database.RunCommand<BsonDocument>(new BsonDocument("buildInfo", 1));
        Assert.Equal(1, result["ok"].AsInt32);
        Assert.True(result.Contains("version"));
    }

    [Fact]
    public void ServerStatus_returns_host()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/serverStatus/
        var result = _fixture.Database.RunCommand<BsonDocument>(new BsonDocument("serverStatus", 1));
        Assert.Equal(1, result["ok"].AsInt32);
        Assert.True(result.Contains("host"));
        Assert.True(result.Contains("connections"));
    }

    [Fact]
    public void HostInfo_returns_system_info()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/hostInfo/
        var result = _fixture.Database.RunCommand<BsonDocument>(new BsonDocument("hostInfo", 1));
        Assert.Equal(1, result["ok"].AsInt32);
        Assert.True(result.Contains("system"));
    }

    [Fact]
    public void ConnectionStatus_returns_ok()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/connectionStatus/
        var result = _fixture.Database.RunCommand<BsonDocument>(new BsonDocument("connectionStatus", 1));
        Assert.Equal(1, result["ok"].AsInt32);
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

        Assert.Equal(1, result["ok"].AsInt32);
        Assert.Equal(3, result["n"].AsInt32);
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

        Assert.Equal(1, result["ok"].AsInt32);
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

        Assert.Equal(1, result["ok"].AsInt32);
        Assert.Equal(2, result["count"].AsInt32);
    }

    [Fact]
    public void DbStats_returns_database_info()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/dbStats/
        var result = _fixture.Database.RunCommand<BsonDocument>(
            new BsonDocument("dbStats", 1));

        Assert.Equal(1, result["ok"].AsInt32);
        Assert.True(result.Contains("collections"));
        Assert.True(result.Contains("objects"));
    }

    [Fact]
    public void Create_command_creates_collection()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/command/create/
        var result = _fixture.Database.RunCommand<BsonDocument>(
            new BsonDocument("create", "rc_created"));

        Assert.Equal(1, result["ok"].AsInt32);

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

        Assert.Equal(1, result["ok"].AsInt32);
    }

    [Fact]
    public async Task RunCommandAsync_works()
    {
        var result = await _fixture.Database.RunCommandAsync<BsonDocument>(
            new BsonDocument("ping", 1));
        Assert.Equal(1, result["ok"].AsInt32);
    }

    [Fact]
    public void Unknown_command_returns_error()
    {
        var result = _fixture.Database.RunCommand<BsonDocument>(
            new BsonDocument("unknownCommand", 1));
        Assert.Equal(0, result["ok"].AsInt32);
        Assert.True(result.Contains("errmsg"));
    }
}
