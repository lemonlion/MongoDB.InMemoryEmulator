using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoDB.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Phase 4 integration tests: Time series collections, SDK drift detector.
/// </summary>
public class MiscellaneousTests
{
    [Fact]
    public async Task TimeSeries_collection_stores_and_queries_documents()
    {
        // Ref: https://www.mongodb.com/docs/manual/core/timeseries-collections/
        //   "Time series collections efficiently store sequences of measurements over a period of time."
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("ts_test");

        // Create time series collection via options
        db.CreateCollection("metrics", new CreateCollectionOptions
        {
            TimeSeriesOptions = new TimeSeriesOptions("timestamp", metaField: "deviceId")
        });

        var collection = db.GetCollection<BsonDocument>("metrics");
        await collection.InsertManyAsync(new[]
        {
            new BsonDocument { { "timestamp", DateTime.UtcNow }, { "deviceId", "d1" }, { "temp", 22.5 } },
            new BsonDocument { { "timestamp", DateTime.UtcNow }, { "deviceId", "d2" }, { "temp", 23.1 } },
        });

        var count = await collection.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);
        Assert.Equal(2, count);

        // Filter by deviceId
        var d1Docs = await collection.Find(
            Builders<BsonDocument>.Filter.Eq("deviceId", "d1")).ToListAsync();
        Assert.Single(d1Docs);
    }

    [Fact]
    public void SdkVersionDriftDetector_returns_null_for_current_version()
    {
        // The current driver version should be in the tested range
        var warning = SdkVersionDriftDetector.Check();
        // We expect null (no warning) since we're testing with the exact version
        Assert.Null(warning);
    }

    [Fact]
    public void SdkVersionDriftDetector_WarnIfDrift_does_not_throw()
    {
        // Just verify it doesn't throw
        SdkVersionDriftDetector.WarnIfDrift();
    }

    [Fact]
    public async Task TimeSeries_collection_via_RunCommand()
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("ts_cmd");

        var result = db.RunCommand<BsonDocument>(new BsonDocument
        {
            { "create", "events" },
            { "timeseries", new BsonDocument
                {
                    { "timeField", "ts" },
                    { "metaField", "source" },
                    { "granularity", "seconds" }
                }
            }
        });

        Assert.Equal(1, result["ok"].AsInt32);

        var collection = db.GetCollection<BsonDocument>("events");
        await collection.InsertOneAsync(new BsonDocument { { "ts", DateTime.UtcNow }, { "source", "api" }, { "value", 42 } });

        var count = await collection.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);
        Assert.Equal(1, count);
    }
}
