using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoDB.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for geospatial filter operators ($geoWithin, $geoIntersects, $near, $nearSphere)
/// and the $geoNear aggregation stage.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/reference/operator/query-geospatial/
/// Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/geoNear/
/// </remarks>
public class GeospatialTests
{
    private static IMongoCollection<BsonDocument> CreateCollection()
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("testdb");
        return db.GetCollection<BsonDocument>("geo");
    }

    private static BsonDocument MakePoint(string name, double lng, double lat)
    {
        return new BsonDocument
        {
            { "_id", name },
            { "name", name },
            { "location", new BsonDocument
                {
                    { "type", "Point" },
                    { "coordinates", new BsonArray { lng, lat } }
                }
            }
        };
    }

    [Fact]
    public void GeoWithin_polygon_returns_contained_points()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/geoWithin/
        var col = CreateCollection();
        // Points: London (approx), Paris (approx), New York (outside)
        col.InsertOne(MakePoint("london", -0.1276, 51.5074));
        col.InsertOne(MakePoint("paris", 2.3522, 48.8566));
        col.InsertOne(MakePoint("new_york", -74.006, 40.7128));

        // Polygon covering Western Europe (rough bounding box)
        var polygon = new BsonDocument
        {
            { "type", "Polygon" },
            { "coordinates", new BsonArray
                {
                    new BsonArray
                    {
                        new BsonArray { -10, 45 },
                        new BsonArray { 10, 45 },
                        new BsonArray { 10, 55 },
                        new BsonArray { -10, 55 },
                        new BsonArray { -10, 45 }
                    }
                }
            }
        };

        var filter = new BsonDocument("location", new BsonDocument("$geoWithin",
            new BsonDocument("$geometry", polygon)));

        var results = col.Find(filter).ToList();

        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r["name"] == "london");
        Assert.Contains(results, r => r["name"] == "paris");
    }

    [Fact]
    public void GeoIntersects_returns_intersecting_geometries()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/geoIntersects/
        var col = CreateCollection();
        // A line from London to Paris
        col.InsertOne(new BsonDocument
        {
            { "_id", "london-paris-line" },
            { "route", new BsonDocument
                {
                    { "type", "LineString" },
                    { "coordinates", new BsonArray
                        {
                            new BsonArray { -0.1276, 51.5074 },
                            new BsonArray { 2.3522, 48.8566 }
                        }
                    }
                }
            }
        });
        // A line from Madrid to Rome (does not cross the above)
        col.InsertOne(new BsonDocument
        {
            { "_id", "madrid-rome-line" },
            { "route", new BsonDocument
                {
                    { "type", "LineString" },
                    { "coordinates", new BsonArray
                        {
                            new BsonArray { -3.7038, 40.4168 },
                            new BsonArray { 12.4964, 41.9028 }
                        }
                    }
                }
            }
        });

        // Query with a polygon that covers the English Channel
        var channelPoly = new BsonDocument
        {
            { "type", "Polygon" },
            { "coordinates", new BsonArray
                {
                    new BsonArray
                    {
                        new BsonArray { -1, 49 },
                        new BsonArray { 3, 49 },
                        new BsonArray { 3, 52 },
                        new BsonArray { -1, 52 },
                        new BsonArray { -1, 49 }
                    }
                }
            }
        };

        var filter = new BsonDocument("route", new BsonDocument("$geoIntersects",
            new BsonDocument("$geometry", channelPoly)));

        var results = col.Find(filter).ToList();
        Assert.Single(results);
        Assert.Equal("london-paris-line", results[0]["_id"].AsString);
    }

    [Fact]
    public void Near_filters_by_maxDistance()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/near/
        var col = CreateCollection();
        col.InsertOne(MakePoint("london", -0.1276, 51.5074));
        col.InsertOne(MakePoint("paris", 2.3522, 48.8566));
        col.InsertOne(MakePoint("tokyo", 139.6917, 35.6895));

        // Near London with max 500km (should include Paris ~340km, exclude Tokyo)
        var filter = new BsonDocument("location", new BsonDocument("$near", new BsonDocument
        {
            { "$geometry", new BsonDocument { { "type", "Point" }, { "coordinates", new BsonArray { -0.1276, 51.5074 } } } },
            { "$maxDistance", 500_000 } // 500km in meters
        }));

        var results = col.Find(filter).ToList();
        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r["name"] == "london");
        Assert.Contains(results, r => r["name"] == "paris");
    }

    [Fact]
    public void NearSphere_filters_by_maxDistance()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/nearSphere/
        var col = CreateCollection();
        col.InsertOne(MakePoint("london", -0.1276, 51.5074));
        col.InsertOne(MakePoint("paris", 2.3522, 48.8566));
        col.InsertOne(MakePoint("new_york", -74.006, 40.7128));

        var filter = new BsonDocument("location", new BsonDocument("$nearSphere", new BsonDocument
        {
            { "$geometry", new BsonDocument { { "type", "Point" }, { "coordinates", new BsonArray { -0.1276, 51.5074 } } } },
            { "$maxDistance", 400_000 } // 400km in meters
        }));

        var results = col.Find(filter).ToList();
        // Only London (0m) and Paris (~340km) should match, not New York (~5500km)
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public void Near_with_minDistance_excludes_close_points()
    {
        var col = CreateCollection();
        col.InsertOne(MakePoint("london", -0.1276, 51.5074));
        col.InsertOne(MakePoint("paris", 2.3522, 48.8566));

        var filter = new BsonDocument("location", new BsonDocument("$near", new BsonDocument
        {
            { "$geometry", new BsonDocument { { "type", "Point" }, { "coordinates", new BsonArray { -0.1276, 51.5074 } } } },
            { "$minDistance", 100_000 }, // 100km
            { "$maxDistance", 500_000 }  // 500km
        }));

        var results = col.Find(filter).ToList();
        Assert.Single(results);
        Assert.Equal("paris", results[0]["name"].AsString);
    }

    [Fact]
    public void GeoNear_aggregation_returns_sorted_by_distance()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/geoNear/
        var col = CreateCollection();
        col.InsertOne(MakePoint("london", -0.1276, 51.5074));
        col.InsertOne(MakePoint("paris", 2.3522, 48.8566));
        col.InsertOne(MakePoint("new_york", -74.006, 40.7128));

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$geoNear", new BsonDocument
            {
                { "near", new BsonDocument { { "type", "Point" }, { "coordinates", new BsonArray { -0.1276, 51.5074 } } } },
                { "distanceField", "dist.calculated" },
                { "spherical", true }
            }));

        var results = col.Aggregate(pipeline).ToList();

        Assert.Equal(3, results.Count);
        // Should be sorted by distance: London, Paris, New York
        Assert.Equal("london", results[0]["name"].AsString);
        Assert.Equal("paris", results[1]["name"].AsString);
        Assert.Equal("new_york", results[2]["name"].AsString);

        // Distance field populated
        Assert.True(results[0]["dist"]["calculated"].ToDouble() < 1); // ~0m
        Assert.True(results[1]["dist"]["calculated"].ToDouble() > 300_000); // ~340km
        Assert.True(results[2]["dist"]["calculated"].ToDouble() > 5_000_000); // ~5500km
    }

    [Fact]
    public void GeoNear_with_maxDistance_limits_results()
    {
        var col = CreateCollection();
        col.InsertOne(MakePoint("london", -0.1276, 51.5074));
        col.InsertOne(MakePoint("paris", 2.3522, 48.8566));
        col.InsertOne(MakePoint("new_york", -74.006, 40.7128));

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$geoNear", new BsonDocument
            {
                { "near", new BsonDocument { { "type", "Point" }, { "coordinates", new BsonArray { -0.1276, 51.5074 } } } },
                { "distanceField", "distance" },
                { "maxDistance", 500_000 },
                { "spherical", true }
            }));

        var results = col.Aggregate(pipeline).ToList();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public void GeoNear_with_query_filter()
    {
        var col = CreateCollection();
        col.InsertOne(new BsonDocument
        {
            { "_id", "london" },
            { "name", "london" },
            { "country", "UK" },
            { "location", new BsonDocument { { "type", "Point" }, { "coordinates", new BsonArray { -0.1276, 51.5074 } } } }
        });
        col.InsertOne(new BsonDocument
        {
            { "_id", "paris" },
            { "name", "paris" },
            { "country", "FR" },
            { "location", new BsonDocument { { "type", "Point" }, { "coordinates", new BsonArray { 2.3522, 48.8566 } } } }
        });

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$geoNear", new BsonDocument
            {
                { "near", new BsonDocument { { "type", "Point" }, { "coordinates", new BsonArray { 0, 50 } } } },
                { "distanceField", "distance" },
                { "query", new BsonDocument("country", "UK") },
                { "key", "location" },
                { "spherical", true }
            }));

        var results = col.Aggregate(pipeline).ToList();
        Assert.Single(results);
        Assert.Equal("london", results[0]["name"].AsString);
    }

    [Fact]
    public void GeoNear_with_distanceMultiplier()
    {
        var col = CreateCollection();
        col.InsertOne(MakePoint("paris", 2.3522, 48.8566));

        var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(
            new BsonDocument("$geoNear", new BsonDocument
            {
                { "near", new BsonDocument { { "type", "Point" }, { "coordinates", new BsonArray { -0.1276, 51.5074 } } } },
                { "distanceField", "distKm" },
                { "distanceMultiplier", 0.001 }, // Convert meters to km
                { "spherical", true }
            }));

        var results = col.Aggregate(pipeline).ToList();
        Assert.Single(results);

        var distKm = results[0]["distKm"].ToDouble();
        Assert.True(distKm > 300 && distKm < 400, $"Expected ~340km, got {distKm}km");
    }

    [Fact]
    public void GeoWithin_centerSphere_legacy()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/centerSphere/
        var col = CreateCollection();
        col.InsertOne(MakePoint("london", -0.1276, 51.5074));
        col.InsertOne(MakePoint("tokyo", 139.6917, 35.6895));

        // $centerSphere with radius in radians (500km / earth radius)
        var filter = new BsonDocument("location", new BsonDocument("$geoWithin",
            new BsonDocument("$centerSphere", new BsonArray
            {
                new BsonArray { -0.1276, 51.5074 },
                500_000.0 / 6_378_100.0 // ~500km in radians
            })));

        var results = col.Find(filter).ToList();
        Assert.Single(results);
        Assert.Equal("london", results[0]["name"].AsString);
    }
}
