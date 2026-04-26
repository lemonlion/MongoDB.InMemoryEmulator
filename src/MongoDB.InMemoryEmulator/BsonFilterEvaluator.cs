using MongoDB.Bson;
using NetTopologySuite.Geometries;

namespace MongoDB.InMemoryEmulator;

/// <summary>
/// Evaluates a rendered BSON filter document against a BSON document.
/// Phase 1: supports _id equality and empty filters.
/// Phase 2 will add all comparison, logical, element, array, and evaluation operators.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/reference/operator/query/
///   "Query selectors define conditions using query operators."
/// </remarks>
internal static class BsonFilterEvaluator
{
    /// <summary>
    /// Returns true if the document matches the filter.
    /// Top-level filter is an implicit $and of all its elements.
    /// </summary>
    public static bool Matches(BsonDocument document, BsonDocument filter, BsonDocument? variables = null)
    {
        foreach (var element in filter)
        {
            if (!MatchesElement(document, element, variables))
                return false;
        }
        return true;
    }

    private static bool MatchesElement(BsonDocument document, BsonElement element, BsonDocument? variables = null)
    {
        return element.Name switch
        {
            // Logical operators
            "$and" => element.Value.AsBsonArray.All(sub => Matches(document, sub.AsBsonDocument, variables)),
            "$or" => element.Value.AsBsonArray.Any(sub => Matches(document, sub.AsBsonDocument, variables)),
            "$nor" => !element.Value.AsBsonArray.Any(sub => Matches(document, sub.AsBsonDocument, variables)),

            // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/expr/
            //   "Allows the use of aggregation expressions within the query language."
            "$expr" => AggregationExpressionEvaluator.IsTruthy(
                AggregationExpressionEvaluator.Evaluate(document, element.Value, variables)),

            // Field-level condition
            _ => MatchesField(document, element.Name, element.Value)
        };
    }

    private static bool MatchesField(BsonDocument document, string fieldPath, BsonValue condition)
    {
        var fieldValue = ResolveFieldPath(document, fieldPath);
        var fieldExists = FieldExists(document, fieldPath);

        if (condition is BsonDocument condDoc && condDoc.ElementCount > 0 && condDoc.Names.First().StartsWith("$"))
        {
            // Operator conditions: { field: { $eq: value, $gt: value, ... } }
            return condDoc.All(op => MatchesOperator(fieldValue, fieldExists, op.Name, op.Value));
        }

        // Implicit equality: { field: value }
        return MatchesEquality(fieldValue, fieldExists, condition);
    }

    private static bool MatchesOperator(BsonValue fieldValue, bool fieldExists, string op, BsonValue operand)
    {
        return op switch
        {
            "$eq" => MatchesEquality(fieldValue, fieldExists, operand),
            "$ne" => !MatchesEquality(fieldValue, fieldExists, operand),
            "$gt" => fieldExists && fieldValue.CompareTo(operand) > 0,
            "$gte" => fieldExists && fieldValue.CompareTo(operand) >= 0,
            "$lt" => fieldExists && fieldValue.CompareTo(operand) < 0,
            "$lte" => fieldExists && fieldValue.CompareTo(operand) <= 0,
            "$in" => MatchesIn(fieldValue, fieldExists, operand.AsBsonArray),
            "$nin" => !MatchesIn(fieldValue, fieldExists, operand.AsBsonArray),
            "$exists" => operand.AsBoolean ? fieldExists : !fieldExists,
            "$type" => fieldExists && MatchesType(fieldValue, operand),
            "$not" => !MatchesField(new BsonDocument("_val", fieldValue), "_val", operand),
            "$regex" => fieldExists && MatchesRegex(fieldValue, operand),

            // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/all/
            //   "The $all operator selects the documents where the value of a field is an array
            //    that contains all the specified elements."
            "$all" => fieldExists && fieldValue is BsonArray allArr &&
                      operand.AsBsonArray.All(required => allArr.Any(el => el.Equals(required))),

            // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/elemMatch/
            //   "The $elemMatch operator matches documents that contain an array field with at least
            //    one element that matches all the specified query criteria."
            "$elemMatch" => fieldExists && MatchesElemMatch(fieldValue, operand.AsBsonDocument),

            // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/size/
            //   "The $size operator matches any array with the number of elements specified."
            "$size" => fieldExists && fieldValue is BsonArray sizeArr && sizeArr.Count == operand.ToInt32(),

            // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/mod/
            //   "Select documents where the value of a field divided by a divisor has the specified remainder."
            "$mod" => fieldExists && MatchesMod(fieldValue, operand.AsBsonArray),

            // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/bitsAllSet/
            "$bitsAllSet" => fieldExists && MatchesBits(fieldValue, operand, "allSet"),
            "$bitsAllClear" => fieldExists && MatchesBits(fieldValue, operand, "allClear"),
            "$bitsAnySet" => fieldExists && MatchesBits(fieldValue, operand, "anySet"),
            "$bitsAnyClear" => fieldExists && MatchesBits(fieldValue, operand, "anyClear"),

            // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/geoWithin/
            //   "Selects documents with geospatial data that exists entirely within a specified shape."
            "$geoWithin" => fieldExists && MatchesGeoWithin(fieldValue, operand.AsBsonDocument),

            // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/geoIntersects/
            //   "Selects documents whose geospatial data intersects with a specified GeoJSON object."
            "$geoIntersects" => fieldExists && MatchesGeoIntersects(fieldValue, operand.AsBsonDocument),

            // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/near/
            //   "Specifies a point for which a geospatial query returns the documents from nearest to farthest."
            "$near" => fieldExists && MatchesNear(fieldValue, operand),
            "$nearSphere" => fieldExists && MatchesNearSphere(fieldValue, operand),

            _ => throw new NotSupportedException($"Filter operator '{op}' is not yet supported.")
        };
    }

    /// <summary>
    /// Ref: https://www.mongodb.com/docs/manual/tutorial/query-for-null-fields/
    ///   "$eq: null matches documents where field is null OR field does not exist."
    /// </summary>
    private static bool MatchesEquality(BsonValue fieldValue, bool fieldExists, BsonValue target)
    {
        if (target == BsonNull.Value || target.IsBsonNull)
            return !fieldExists || fieldValue == BsonNull.Value || fieldValue.IsBsonNull;

        // Direct equality
        if (fieldValue.Equals(target)) return true;

        // Array implicit iteration: if field is array, check each element
        if (fieldValue is BsonArray array)
            return array.Any(element => element.Equals(target));

        return false;
    }

    private static bool MatchesIn(BsonValue fieldValue, bool fieldExists, BsonArray values)
    {
        return values.Any(v => MatchesEquality(fieldValue, fieldExists, v));
    }

    private static bool MatchesType(BsonValue fieldValue, BsonValue typeSpec)
    {
        if (typeSpec is BsonString typeStr)
        {
            return typeStr.Value switch
            {
                "double" => fieldValue.BsonType == BsonType.Double,
                "string" => fieldValue.BsonType == BsonType.String,
                "object" => fieldValue.BsonType == BsonType.Document,
                "array" => fieldValue.BsonType == BsonType.Array,
                "binData" => fieldValue.BsonType == BsonType.Binary,
                "objectId" => fieldValue.BsonType == BsonType.ObjectId,
                "bool" => fieldValue.BsonType == BsonType.Boolean,
                "date" => fieldValue.BsonType == BsonType.DateTime,
                "null" => fieldValue.BsonType == BsonType.Null,
                "regex" => fieldValue.BsonType == BsonType.RegularExpression,
                "int" => fieldValue.BsonType == BsonType.Int32,
                "long" => fieldValue.BsonType == BsonType.Int64,
                "decimal" => fieldValue.BsonType == BsonType.Decimal128,
                "number" => fieldValue.IsNumeric,
                _ => false
            };
        }

        if (typeSpec is BsonInt32 typeInt)
        {
            return (int)fieldValue.BsonType == typeInt.Value;
        }

        return false;
    }

    private static bool MatchesRegex(BsonValue fieldValue, BsonValue regex)
    {
        if (fieldValue.BsonType != BsonType.String) return false;

        var pattern = regex is BsonRegularExpression bre ? bre.Pattern : regex.AsString;
        var options = regex is BsonRegularExpression bre2 ? bre2.Options : "";

        var regexOptions = System.Text.RegularExpressions.RegexOptions.None;
        if (options.Contains('i')) regexOptions |= System.Text.RegularExpressions.RegexOptions.IgnoreCase;
        if (options.Contains('m')) regexOptions |= System.Text.RegularExpressions.RegexOptions.Multiline;
        if (options.Contains('s')) regexOptions |= System.Text.RegularExpressions.RegexOptions.Singleline;
        if (options.Contains('x')) regexOptions |= System.Text.RegularExpressions.RegexOptions.IgnorePatternWhitespace;

        return System.Text.RegularExpressions.Regex.IsMatch(fieldValue.AsString, pattern, regexOptions);
    }

    internal static BsonValue ResolveFieldPath(BsonDocument doc, string path)
    {
        var parts = path.Split('.');
        BsonValue current = doc;

        foreach (var part in parts)
        {
            if (current is BsonDocument nested)
            {
                if (!nested.Contains(part))
                    return BsonNull.Value;
                current = nested[part];
            }
            else if (current is BsonArray array && int.TryParse(part, out var index))
            {
                current = index < array.Count ? array[index] : BsonNull.Value;
            }
            else
            {
                return BsonNull.Value;
            }
        }
        return current;
    }

    private static bool FieldExists(BsonDocument doc, string path)
    {
        var parts = path.Split('.');
        BsonValue current = doc;

        foreach (var part in parts)
        {
            if (current is BsonDocument nested)
            {
                if (!nested.Contains(part))
                    return false;
                current = nested[part];
            }
            else if (current is BsonArray array && int.TryParse(part, out var index))
            {
                if (index >= array.Count) return false;
                current = array[index];
            }
            else
            {
                return false;
            }
        }
        return true;
    }

    private static bool MatchesElemMatch(BsonValue fieldValue, BsonDocument criteria)
    {
        if (fieldValue is not BsonArray array) return false;
        return array.Any(element =>
        {
            if (element is BsonDocument elementDoc)
                return Matches(elementDoc, criteria);
            // For scalar elements, wrap in a doc and match against operator conditions
            var wrapped = new BsonDocument("_val", element);
            var wrappedCriteria = new BsonDocument("_val", criteria);
            return Matches(wrapped, wrappedCriteria);
        });
    }

    private static bool MatchesMod(BsonValue fieldValue, BsonArray operand)
    {
        if (!fieldValue.IsNumeric) return false;
        var divisor = operand[0].ToDouble();
        var remainder = operand[1].ToDouble();
        return Math.Abs(fieldValue.ToDouble() % divisor - remainder) < 0.0001;
    }

    private static bool MatchesBits(BsonValue fieldValue, BsonValue bitmask, string mode)
    {
        if (!fieldValue.IsNumeric) return false;
        long value = fieldValue.ToInt64();
        long mask;

        if (bitmask is BsonArray positions)
        {
            // Bit positions array
            mask = 0;
            foreach (var pos in positions)
                mask |= 1L << pos.ToInt32();
        }
        else
        {
            mask = bitmask.ToInt64();
        }

        return mode switch
        {
            "allSet" => (value & mask) == mask,
            "allClear" => (value & mask) == 0,
            "anySet" => (value & mask) != 0,
            "anyClear" => (value & mask) != mask,
            _ => false
        };
    }

    private static bool MatchesGeoWithin(BsonValue fieldValue, BsonDocument operand)
    {
        var fieldGeom = GeoJsonHelper.ToGeometry(fieldValue.AsBsonDocument);
        if (fieldGeom == null) return false;

        // $geoWithin: { $geometry: { type: ..., coordinates: ... } }
        if (operand.Contains("$geometry"))
        {
            var outerGeom = GeoJsonHelper.ToGeometry(operand["$geometry"].AsBsonDocument);
            return outerGeom != null && GeoJsonHelper.IsWithin(fieldGeom, outerGeom);
        }

        // Legacy shapes: $box, $polygon, $center, $centerSphere
        if (operand.Contains("$centerSphere"))
        {
            var arr = operand["$centerSphere"].AsBsonArray;
            var center = GeoJsonHelper.ExtractPoint(arr[0]);
            var radiusRadians = arr[1].ToDouble();
            if (center == null) return false;
            var radiusMeters = radiusRadians * 6_378_100.0;
            return GeoJsonHelper.DistanceMeters(center, fieldGeom) <= radiusMeters;
        }

        return false;
    }

    private static bool MatchesGeoIntersects(BsonValue fieldValue, BsonDocument operand)
    {
        var fieldGeom = GeoJsonHelper.ToGeometry(fieldValue.AsBsonDocument);
        if (fieldGeom == null) return false;

        if (operand.Contains("$geometry"))
        {
            var otherGeom = GeoJsonHelper.ToGeometry(operand["$geometry"].AsBsonDocument);
            return otherGeom != null && GeoJsonHelper.Intersects(fieldGeom, otherGeom);
        }

        return false;
    }

    private static bool MatchesNear(BsonValue fieldValue, BsonValue operand)
    {
        return MatchesNearCore(fieldValue, operand);
    }

    private static bool MatchesNearSphere(BsonValue fieldValue, BsonValue operand)
    {
        return MatchesNearCore(fieldValue, operand);
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/near/
    //   "$near returns documents in order of proximity. $maxDistance and $minDistance in meters for GeoJSON."
    private static bool MatchesNearCore(BsonValue fieldValue, BsonValue operand)
    {
        Coordinate? target;
        double maxDistance = double.MaxValue;
        double minDistance = 0;

        if (operand is BsonDocument nearDoc)
        {
            if (nearDoc.Contains("$geometry"))
            {
                target = GeoJsonHelper.ExtractPoint(nearDoc["$geometry"]);
                if (nearDoc.Contains("$maxDistance")) maxDistance = nearDoc["$maxDistance"].ToDouble();
                if (nearDoc.Contains("$minDistance")) minDistance = nearDoc["$minDistance"].ToDouble();
            }
            else
            {
                target = GeoJsonHelper.ExtractPoint(nearDoc);
            }
        }
        else
        {
            target = GeoJsonHelper.ExtractPoint(operand);
        }

        if (target == null) return false;

        var fieldPoint = GeoJsonHelper.ExtractPoint(fieldValue);
        if (fieldPoint == null)
        {
            var fieldGeom = GeoJsonHelper.ToGeometry(fieldValue.AsBsonDocument);
            if (fieldGeom == null) return false;
            var dist = GeoJsonHelper.DistanceMeters(target, fieldGeom);
            return dist >= minDistance && dist <= maxDistance;
        }
        else
        {
            var dist = GeoJsonHelper.HaversineDistance(target, fieldPoint);
            return dist >= minDistance && dist <= maxDistance;
        }
    }
}
