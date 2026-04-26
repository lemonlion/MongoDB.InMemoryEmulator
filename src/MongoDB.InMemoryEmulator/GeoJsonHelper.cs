using MongoDB.Bson;
using NetTopologySuite.Geometries;

namespace MongoDB.InMemoryEmulator;

/// <summary>
/// Converts GeoJSON BSON documents to NetTopologySuite geometries and provides
/// distance/containment/intersection calculations for geospatial query support.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/reference/geojson/
///   "MongoDB supports GeoJSON objects for geospatial data."
/// Ref: https://www.mongodb.com/docs/manual/reference/operator/query-geospatial/
/// </remarks>
internal static class GeoJsonHelper
{
    private static readonly GeometryFactory Factory = new(new PrecisionModel(), 4326);

    // Ref: https://www.mongodb.com/docs/manual/reference/geojson/
    //   "GeoJSON objects have a type field and a coordinates field."
    internal static Geometry? ToGeometry(BsonValue value)
    {
        if (value is not BsonDocument doc) return null;
        if (!doc.Contains("type") || !doc.Contains("coordinates")) return null;

        var type = doc["type"].AsString;
        var coords = doc["coordinates"];

        return type switch
        {
            "Point" => ToPoint(coords),
            "LineString" => ToLineString(coords),
            "Polygon" => ToPolygon(coords),
            "MultiPoint" => ToMultiPoint(coords),
            "MultiLineString" => ToMultiLineString(coords),
            "MultiPolygon" => ToMultiPolygon(coords),
            "GeometryCollection" => ToGeometryCollection(doc),
            _ => null
        };
    }

    private static Point ToPoint(BsonValue coords)
    {
        var arr = coords.AsBsonArray;
        return Factory.CreatePoint(new Coordinate(arr[0].ToDouble(), arr[1].ToDouble()));
    }

    private static LineString ToLineString(BsonValue coords)
    {
        var arr = coords.AsBsonArray;
        var coordinates = arr.Select(c => new Coordinate(c[0].ToDouble(), c[1].ToDouble())).ToArray();
        return Factory.CreateLineString(coordinates);
    }

    private static Polygon ToPolygon(BsonValue coords)
    {
        var rings = coords.AsBsonArray;
        var shell = Factory.CreateLinearRing(
            rings[0].AsBsonArray.Select(c => new Coordinate(c[0].ToDouble(), c[1].ToDouble())).ToArray());

        var holes = new LinearRing[rings.Count - 1];
        for (int i = 1; i < rings.Count; i++)
        {
            holes[i - 1] = Factory.CreateLinearRing(
                rings[i].AsBsonArray.Select(c => new Coordinate(c[0].ToDouble(), c[1].ToDouble())).ToArray());
        }

        return Factory.CreatePolygon(shell, holes);
    }

    private static MultiPoint ToMultiPoint(BsonValue coords)
    {
        var arr = coords.AsBsonArray;
        var points = arr.Select(c => ToPoint(c)).ToArray();
        return Factory.CreateMultiPoint(points);
    }

    private static MultiLineString ToMultiLineString(BsonValue coords)
    {
        var arr = coords.AsBsonArray;
        var lines = arr.Select(c => ToLineString(c)).ToArray();
        return Factory.CreateMultiLineString(lines);
    }

    private static MultiPolygon ToMultiPolygon(BsonValue coords)
    {
        var arr = coords.AsBsonArray;
        var polygons = arr.Select(c => ToPolygon(c)).ToArray();
        return Factory.CreateMultiPolygon(polygons);
    }

    private static GeometryCollection ToGeometryCollection(BsonDocument doc)
    {
        if (!doc.Contains("geometries")) return Factory.CreateGeometryCollection();
        var geometries = doc["geometries"].AsBsonArray
            .Select(g => ToGeometry(g))
            .Where(g => g != null)
            .Cast<Geometry>()
            .ToArray();
        return Factory.CreateGeometryCollection(geometries);
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/near/#behavior
    //   "Specifies a point for which a geospatial query returns the documents from nearest to farthest."
    // Distances are in meters for 2dsphere (WGS84).
    private const double EarthRadiusMeters = 6_378_100.0;

    /// <summary>
    /// Calculates the great-circle distance in meters between two points (Haversine formula).
    /// </summary>
    internal static double HaversineDistance(Coordinate a, Coordinate b)
    {
        var dLat = DegreesToRadians(b.Y - a.Y);
        var dLon = DegreesToRadians(b.X - a.X);
        var lat1 = DegreesToRadians(a.Y);
        var lat2 = DegreesToRadians(b.Y);

        var h = Math.Sin(dLat / 2) * Math.Sin(dLat / 2) +
                Math.Cos(lat1) * Math.Cos(lat2) *
                Math.Sin(dLon / 2) * Math.Sin(dLon / 2);
        var c = 2 * Math.Atan2(Math.Sqrt(h), Math.Sqrt(1 - h));
        return EarthRadiusMeters * c;
    }

    /// <summary>
    /// Returns the minimum great-circle distance between a point and a geometry, in meters.
    /// </summary>
    internal static double DistanceMeters(Coordinate point, Geometry geometry)
    {
        if (geometry is Point p)
            return HaversineDistance(point, p.Coordinate);

        // For non-point geometries, compute distance to nearest coordinate
        var minDist = double.MaxValue;
        foreach (var coord in geometry.Coordinates)
        {
            var d = HaversineDistance(point, coord);
            if (d < minDist) minDist = d;
        }
        return minDist;
    }

    /// <summary>
    /// Checks if a geometry is contained within another geometry using NTS operations.
    /// For spherical operations, we use planar approximation (sufficient for most test scenarios).
    /// </summary>
    internal static bool IsWithin(Geometry inner, Geometry outer)
    {
        return outer.Contains(inner);
    }

    /// <summary>
    /// Checks if two geometries intersect.
    /// </summary>
    internal static bool Intersects(Geometry a, Geometry b)
    {
        return a.Intersects(b);
    }

    /// <summary>
    /// Extracts the center coordinate from a GeoJSON geometry (for $near/$geoNear).
    /// </summary>
    internal static Coordinate? GetCentroid(BsonValue geoJson)
    {
        var geom = ToGeometry(geoJson);
        if (geom == null) return null;
        var centroid = geom.Centroid;
        return centroid?.Coordinate;
    }

    /// <summary>
    /// Extracts a legacy coordinate pair [lng, lat] to a Coordinate.
    /// Also handles { type: "Point", coordinates: [lng, lat] } format.
    /// </summary>
    internal static Coordinate? ExtractPoint(BsonValue value)
    {
        if (value is BsonArray arr && arr.Count >= 2)
            return new Coordinate(arr[0].ToDouble(), arr[1].ToDouble());

        if (value is BsonDocument doc)
        {
            if (doc.Contains("type") && doc["type"].AsString == "Point" && doc.Contains("coordinates"))
            {
                var coords = doc["coordinates"].AsBsonArray;
                return new Coordinate(coords[0].ToDouble(), coords[1].ToDouble());
            }
        }

        return null;
    }

    private static double DegreesToRadians(double degrees) => degrees * Math.PI / 180.0;
}
