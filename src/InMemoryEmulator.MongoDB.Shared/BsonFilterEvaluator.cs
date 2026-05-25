using global::MongoDB.Bson;
using NetTopologySuite.Geometries;

namespace InMemoryEmulator.MongoDB;

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

            // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/text/
            //   "Performs text search on the content of the fields indexed with a text index."
            "$text" => MatchesText(document, element.Value.AsBsonDocument),

            // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/expr/
            //   "Allows the use of aggregation expressions within the query language."
            "$expr" => AggregationExpressionEvaluator.IsTruthy(
                AggregationExpressionEvaluator.Evaluate(document, element.Value, variables)),

            // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/jsonSchema/
            //   "Matches documents that satisfy the specified JSON Schema."
            "$jsonSchema" => MatchesJsonSchema(document, element.Value.AsBsonDocument),

            // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/where/
            //   "Use the $where operator to pass either a string containing a JavaScript expression
            //    or a full JavaScript function to the query system."
            "$where" => EvaluateWhere(document, element.Value),

            // Field-level condition
            _ => MatchesField(document, element.Name, element.Value)
        };
    }

    private static bool MatchesField(BsonDocument document, string fieldPath, BsonValue condition)
    {
        var fieldValue = ResolveFieldPath(document, fieldPath);
        var fieldExists = FieldExists(document, fieldPath);

        // Ref: https://www.mongodb.com/docs/manual/tutorial/query-array-of-documents/
        //   "Use dot notation to query a field in a document nested in an array."
        //   When a dot-notation path traverses through an array of documents,
        //   MongoDB checks each array element against the remaining path.
        if (!fieldExists && fieldPath.Contains('.'))
        {
            if (TryMatchFieldThroughArray(document, fieldPath, condition))
                return true;
        }

        if (condition is BsonDocument condDoc && condDoc.ElementCount > 0 && condDoc.Names.First().StartsWith("$"))
        {
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/regex/
            //   "$options: Optional. Modifies the $regex behavior. Consumed alongside $regex."
            // Pre-merge $options into $regex so MatchesOperator doesn't see $options as unknown.
            BsonValue? mergedRegex = null;
            if (condDoc.Contains("$regex") && condDoc.Contains("$options"))
            {
                var pattern = condDoc["$regex"].AsString;
                var opts = condDoc["$options"].AsString;
                mergedRegex = new BsonRegularExpression(pattern, opts);
            }

            // Operator conditions: { field: { $eq: value, $gt: value, ... } }
            return condDoc.All(op =>
            {
                if (op.Name == "$options") return true; // consumed by $regex
                if (op.Name == "$regex" && mergedRegex != null)
                    return MatchesOperator(fieldValue, fieldExists, "$regex", mergedRegex);
                return MatchesOperator(fieldValue, fieldExists, op.Name, op.Value);
            });
        }

        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/regex/
        //   "{ field: /pattern/ } is equivalent to { field: { $regex: /pattern/ } }"
        //   Implicit regex matching uses regex semantics, not equality.
        if (condition is BsonRegularExpression)
        {
            return fieldExists && MatchesRegex(fieldValue, condition);
        }

        // Implicit equality: { field: value }
        return MatchesEquality(fieldValue, fieldExists, condition);
    }

    /// <summary>
    /// Handles dot-notation field paths that traverse through arrays of documents.
    /// When a path segment reaches an array and the next part is not a numeric index,
    /// each document element of the array is checked against the remaining path.
    /// </summary>
    private static bool TryMatchFieldThroughArray(BsonDocument document, string fieldPath, BsonValue condition)
    {
        var parts = fieldPath.Split('.');
        BsonValue current = document;

        for (int i = 0; i < parts.Length; i++)
        {
            if (current is BsonDocument nested)
            {
                if (!nested.Contains(parts[i]))
                    return false;
                current = nested[parts[i]];
            }
            else if (current is BsonArray array)
            {
                if (int.TryParse(parts[i], out var index))
                {
                    if (index >= array.Count) return false;
                    current = array[index];
                }
                else
                {
                    // Non-numeric path through array: check each document element
                    var remainingPath = string.Join(".", parts.Skip(i));
                    return array.Any(elem =>
                        elem is BsonDocument elemDoc && MatchesField(elemDoc, remainingPath, condition));
                }
            }
            else
            {
                return false;
            }
        }

        return false;
    }

    private static bool MatchesOperator(BsonValue fieldValue, bool fieldExists, string op, BsonValue operand)
    {
        return op switch
        {
            "$eq" => MatchesEquality(fieldValue, fieldExists, operand),
            "$ne" => !MatchesEquality(fieldValue, fieldExists, operand),

            // Ref: https://www.mongodb.com/docs/manual/tutorial/query-arrays/
            //   "When the field holds an array, comparison operators match the document
            //    if at least one array element meets the condition."
            // Ref: https://www.mongodb.com/docs/manual/tutorial/query-for-null-fields/
            //   When operand is null, missing fields are treated as null for comparison.
            //   null >= null and null <= null are true, so $gte/$lte null match missing fields.
            "$gt" => MatchesComparisonWithNull(fieldValue, fieldExists, operand, (a, b) => a.CompareTo(b) > 0),
            "$gte" => MatchesComparisonWithNull(fieldValue, fieldExists, operand, (a, b) => a.CompareTo(b) >= 0),
            "$lt" => MatchesComparisonWithNull(fieldValue, fieldExists, operand, (a, b) => a.CompareTo(b) < 0),
            "$lte" => MatchesComparisonWithNull(fieldValue, fieldExists, operand, (a, b) => a.CompareTo(b) <= 0),
            "$in" => MatchesIn(fieldValue, fieldExists, operand.AsBsonArray),
            "$nin" => !MatchesIn(fieldValue, fieldExists, operand.AsBsonArray),
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/exists/
            //   "$exists accepts truthy/falsy values (0/1, true/false, null)."
            "$exists" => operand.ToBoolean() ? fieldExists : !fieldExists,
            "$type" => fieldExists && MatchesType(fieldValue, operand),
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/not/
            //   "$not performs a logical NOT operation on the specified <operator-expression>."
            //   If the field doesn't exist, comparison operators return false, so $not returns true.
            "$not" => !MatchesNotInner(fieldValue, fieldExists, operand),
            "$regex" => fieldExists && MatchesRegex(fieldValue, operand),

            // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/all/
            //   "The $all operator selects the documents where the value of a field is an array
            //    that contains all the specified elements."
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/all/
            //   "Use $all with $elemMatch to match documents where the array field contains
            //    at least one element that matches each $elemMatch condition."
            //   "{ tags: { $all: ['ssl'] } } is equivalent to { $and: [ { tags: 'ssl' } ] }"
            //   This means $all also matches scalar values (since { tags: 'ssl' } matches scalars).
            "$all" => fieldExists && operand.AsBsonArray.All(required =>
                      {
                          if (required is BsonDocument reqDoc && reqDoc.ElementCount == 1 && reqDoc.GetElement(0).Name == "$elemMatch")
                              return MatchesElemMatch(fieldValue, reqDoc["$elemMatch"].AsBsonDocument);
                          // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/all/
                          //   "{ tags: { $all: [/^ssl/] } }" — regex elements perform regex matching.
                          if (required is BsonRegularExpression regex)
                              return MatchesRegex(fieldValue, regex);
                          // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/all/
                          //   MongoDB uses cross-type numeric comparison (5 == 5.0 == 5L).
                          if (fieldValue is BsonArray allArr)
                              return allArr.Any(el => BsonValueComparer.Instance.Compare(el, required) == 0);
                          return BsonValueComparer.Instance.Compare(fieldValue, required) == 0;
                      }),

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

        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/in/
        //   "You can also specify regular expression objects in the array."
        //   When the target is a regex, use regex matching instead of equality.
        if (target is BsonRegularExpression)
            return fieldExists && MatchesRegex(fieldValue, target);

        // Direct equality
        if (fieldValue.Equals(target)) return true;

        // Array implicit iteration: if field is array, check each element
        if (fieldValue is BsonArray array)
            return array.Any(element => element.Equals(target));

        return false;
    }

    /// <summary>
    /// Compares a field value against an operand, with array element iteration.
    /// </summary>
    /// <remarks>
    /// <summary>
    /// Evaluates the inner operand of a $not expression using the original fieldValue and fieldExists,
    /// preserving correct behavior for missing fields.
    /// </summary>
    private static bool MatchesNotInner(BsonValue fieldValue, bool fieldExists, BsonValue operand)
    {
        if (operand is BsonRegularExpression regex)
            return fieldExists && MatchesRegex(fieldValue, regex);

        if (operand is BsonDocument condDoc)
        {
            return condDoc.All(op =>
                MatchesOperator(fieldValue, fieldExists, op.Name, op.Value));
        }

        return false;
    }

    /// <summary>
    /// Compares a field value against an operand, handling array elements.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/tutorial/query-arrays/
    ///   "When the field holds an array, comparison operators match the document
    ///    if at least one array element meets the condition."
    /// </remarks>
    private static bool MatchesComparison(BsonValue fieldValue, BsonValue operand, Func<BsonValue, BsonValue, bool> compare)
    {
        if (fieldValue is BsonArray array)
            return array.Any(el => compare(el, operand));

        return compare(fieldValue, operand);
    }

    /// <summary>
    /// Comparison handler that correctly treats missing fields as null when operand is null.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/tutorial/query-for-null-fields/
    ///   Missing fields are treated as null for comparison purposes.
    ///   Real MongoDB 7.0: {$gt: null} and {$lt: null} match NOTHING.
    ///   {$gte: null} and {$lte: null} match only documents where the field is null or missing.
    /// </remarks>
    private static bool MatchesComparisonWithNull(BsonValue fieldValue, bool fieldExists, BsonValue operand, Func<BsonValue, BsonValue, bool> compare)
    {
        // When operand is null, treat missing fields as null for the comparison
        if ((operand == BsonNull.Value || operand.IsBsonNull) && !fieldExists)
            return compare(BsonNull.Value, BsonNull.Value);

        // Ref: Observed real MongoDB 7.0:
        //   When operand is null and field exists with a non-null value,
        //   strict comparisons ($gt/$lt) never match. Only $gte/$lte null match null/missing.
        if ((operand == BsonNull.Value || operand.IsBsonNull) && fieldExists
            && fieldValue != BsonNull.Value && !fieldValue.IsBsonNull)
            return false;

        if (!fieldExists)
            return false;

        return MatchesComparison(fieldValue, operand, compare);
    }

    private static bool MatchesIn(BsonValue fieldValue, bool fieldExists, BsonArray values)
    {
        return values.Any(v => MatchesEquality(fieldValue, fieldExists, v));
    }

    private static bool MatchesType(BsonValue fieldValue, BsonValue typeSpec)
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/type/
        //   "$type can also accept an array of types to check."
        //   Handle array of type specifiers first, delegating each to MatchesType
        //   so that the "array" type check is handled correctly per element.
        if (typeSpec is BsonArray typeArr)
            return typeArr.Any(t => MatchesType(fieldValue, t));

        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/type/
        //   "If the field holds an array, $type returns documents in which at least
        //    one array element matches the specified BSON type."
        if (fieldValue is BsonArray array && !IsTypeCheckForArray(typeSpec))
            return array.Any(el => MatchesTypeSingle(el, typeSpec));

        return MatchesTypeSingle(fieldValue, typeSpec);
    }

    private static bool IsTypeCheckForArray(BsonValue typeSpec)
    {
        if (typeSpec is BsonString ts) return ts.Value == "array";
        if (typeSpec is BsonInt32 ti) return ti.Value == (int)BsonType.Array;
        return false;
    }

    private static bool MatchesTypeSingle(BsonValue fieldValue, BsonValue typeSpec)
    {
        if (typeSpec is BsonString typeStr)
        {
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/type/#available-types
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
                "timestamp" => fieldValue.BsonType == BsonType.Timestamp,
                "javascript" => fieldValue.BsonType == BsonType.JavaScript,
                "javascriptWithScope" => fieldValue.BsonType == BsonType.JavaScriptWithScope,
                "minKey" => fieldValue.BsonType == BsonType.MinKey,
                "maxKey" => fieldValue.BsonType == BsonType.MaxKey,
                "undefined" => fieldValue.BsonType == BsonType.Undefined,
                _ => false
            };
        }

        if (typeSpec is BsonInt32 typeInt)
        {
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/type/
            //   "Numeric BSON type codes can also be used."
            var checkType = typeInt.Value;
            if (checkType == -1) return fieldValue.BsonType == BsonType.MinKey;
            if (checkType == 127) return fieldValue.BsonType == BsonType.MaxKey;
            return (int)fieldValue.BsonType == checkType;
        }

        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/type/
        //   "$type can also accept an array of types to check."
        if (typeSpec is BsonArray typeArr)
            return typeArr.Any(t => MatchesTypeSingle(fieldValue, t));

        return false;
    }

    private static bool MatchesRegex(BsonValue fieldValue, BsonValue regex)
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/regex/
        //   "If the field contains an array, $regex selects only the documents
        //    where at least one element matches the expression."
        if (fieldValue is BsonArray array)
            return array.Any(el => MatchesRegexSingle(el, regex));

        return MatchesRegexSingle(fieldValue, regex);
    }

    private static bool MatchesRegexSingle(BsonValue fieldValue, BsonValue regex)
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

    /// <summary>
    /// Resolves a dot-notation field path, traversing arrays of subdocuments.
    /// Returns all leaf values found across array elements.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/core/document/#dot-notation
    ///   "MongoDB uses the dot notation to access the elements of an array
    ///    and to access the fields of an embedded document."
    ///   When a path segment reaches an array of documents, each document
    ///   is checked for the remaining path segments.
    /// </remarks>
    internal static List<BsonValue> ResolveFieldPathThroughArrays(BsonDocument doc, string path)
    {
        var results = new List<BsonValue>();
        ResolveFieldPathThroughArraysRecursive(doc, path.Split('.'), 0, results);
        return results;
    }

    private static void ResolveFieldPathThroughArraysRecursive(BsonValue current, string[] parts, int index, List<BsonValue> results)
    {
        if (index >= parts.Length)
        {
            results.Add(current);
            return;
        }

        var part = parts[index];

        if (current is BsonDocument nested)
        {
            if (!nested.Contains(part))
            {
                results.Add(BsonNull.Value);
                return;
            }
            ResolveFieldPathThroughArraysRecursive(nested[part], parts, index + 1, results);
        }
        else if (current is BsonArray array)
        {
            if (int.TryParse(part, out var arrIndex))
            {
                if (arrIndex < array.Count)
                    ResolveFieldPathThroughArraysRecursive(array[arrIndex], parts, index + 1, results);
                else
                    results.Add(BsonNull.Value);
            }
            else
            {
                // Non-numeric path through array: traverse each element
                foreach (var element in array)
                {
                    ResolveFieldPathThroughArraysRecursive(element, parts, index, results);
                }
            }
        }
        else
        {
            results.Add(BsonNull.Value);
        }
    }

    internal static bool FieldExists(BsonDocument doc, string path)
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
            // For scalar elements, handle logical operators at top level specially
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/elemMatch/
            //   Logical operators ($or, $and, $nor) apply to the element context.
            return MatchesScalarElemMatch(element, criteria);
        });
    }

    private static bool MatchesScalarElemMatch(BsonValue element, BsonDocument criteria)
    {
        foreach (var crit in criteria)
        {
            bool result = crit.Name switch
            {
                "$or" => crit.Value.AsBsonArray.Any(sub => MatchesScalarElemMatch(element, sub.AsBsonDocument)),
                "$and" => crit.Value.AsBsonArray.All(sub => MatchesScalarElemMatch(element, sub.AsBsonDocument)),
                "$nor" => !crit.Value.AsBsonArray.Any(sub => MatchesScalarElemMatch(element, sub.AsBsonDocument)),
                "$not" => !MatchesScalarElemMatch(element, crit.Value.AsBsonDocument),
                _ => MatchesScalarCondition(element, crit.Name, crit.Value)
            };
            if (!result) return false;
        }
        return true;
    }

    private static bool MatchesScalarCondition(BsonValue element, string op, BsonValue operand)
    {
        // Wrap scalar in a document and use MatchesOperator
        return MatchesOperator(element, true, op, operand);
    }

    private static bool MatchesMod(BsonValue fieldValue, BsonArray operand)
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/mod/
        //   "If the field contains an array, $mod selects the documents whose
        //    arrays contain at least one element that meets the condition."
        if (fieldValue is BsonArray array)
            return array.Any(el => MatchesModSingle(el, operand));

        return MatchesModSingle(fieldValue, operand);
    }

    private static bool MatchesModSingle(BsonValue fieldValue, BsonArray operand)
    {
        if (!fieldValue.IsNumeric) return false;
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/mod/
        //   "If a value is not an integer, the $mod rounds towards zero to the
        //    nearest integer before performing the operation."
        var divisor = (long)Math.Truncate(operand[0].ToDouble());
        var remainder = (long)Math.Truncate(operand[1].ToDouble());
        if (divisor == 0) return false;
        var val = (long)Math.Truncate(fieldValue.ToDouble());
        return val % divisor == remainder;
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

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/text/
    //   "{ $text: { $search: <string>, $language: <string>, $caseSensitive: <boolean>, $diacriticSensitive: <boolean> } }"
    //   "Performs a text search on string fields that have a text index."
    private static bool MatchesText(BsonDocument document, BsonDocument textSpec)
    {
        var searchStr = textSpec["$search"].AsString;
        var caseSensitive = textSpec.GetValue("$caseSensitive", false).ToBoolean();

        // Extract all string fields from the document
        var allText = ExtractAllStringValues(document);
        var textContent = string.Join(" ", allText);

        if (!caseSensitive)
            textContent = textContent.ToLowerInvariant();

        // Parse search string: handle phrases (quoted), negation (- prefix), and terms
        var (requiredTerms, requiredPhrases, negatedTerms) = ParseTextSearch(searchStr);

        // Check negated terms first
        foreach (var neg in negatedTerms)
        {
            var check = caseSensitive ? neg : neg.ToLowerInvariant();
            if (textContent.Contains(check))
                return false;
        }

        // If only negations, match all non-negated docs
        if (requiredTerms.Count == 0 && requiredPhrases.Count == 0)
            return true;

        // Check phrases
        foreach (var phrase in requiredPhrases)
        {
            var check = caseSensitive ? phrase : phrase.ToLowerInvariant();
            if (!textContent.Contains(check))
                return false;
        }

        // Check terms (OR logic for individual terms)
        if (requiredTerms.Count > 0)
        {
            return requiredTerms.Any(term =>
            {
                var check = caseSensitive ? term : term.ToLowerInvariant();
                return textContent.Contains(check);
            });
        }

        return true;
    }

    /// <summary>
    /// Calculates a simple text relevance score for a document against a search string.
    /// Used for $meta: "textScore" projections.
    /// </summary>
    internal static double CalculateTextScore(BsonDocument document, string searchStr)
    {
        var allText = ExtractAllStringValues(document);
        var textContent = string.Join(" ", allText).ToLowerInvariant();
        var (terms, phrases, _) = ParseTextSearch(searchStr);

        double score = 0;
        foreach (var term in terms)
        {
            var lower = term.ToLowerInvariant();
            int idx = 0;
            while ((idx = textContent.IndexOf(lower, idx, StringComparison.Ordinal)) >= 0)
            {
                score += 1.0;
                idx += lower.Length;
            }
        }
        foreach (var phrase in phrases)
        {
            var lower = phrase.ToLowerInvariant();
            if (textContent.Contains(lower))
                score += 5.0; // Phrase matches score higher
        }

        return score;
    }

    private static List<string> ExtractAllStringValues(BsonDocument doc)
    {
        var results = new List<string>();
        foreach (var element in doc)
        {
            if (element.Name.StartsWith("_")) continue; // Skip system fields
            if (element.Value.IsString)
                results.Add(element.Value.AsString);
            else if (element.Value is BsonDocument nested)
                results.AddRange(ExtractAllStringValues(nested));
            else if (element.Value is BsonArray arr)
            {
                foreach (var item in arr)
                {
                    if (item.IsString)
                        results.Add(item.AsString);
                    else if (item is BsonDocument nestedDoc)
                        results.AddRange(ExtractAllStringValues(nestedDoc));
                }
            }
        }
        return results;
    }

    private static (List<string> terms, List<string> phrases, List<string> negated) ParseTextSearch(string search)
    {
        var terms = new List<string>();
        var phrases = new List<string>();
        var negated = new List<string>();

        bool inQuote = false;
        var current = new System.Text.StringBuilder();
        bool isNegated = false;

        for (int i = 0; i < search.Length; i++)
        {
            var c = search[i];
            if (c == '"')
            {
                if (inQuote)
                {
                    // End of phrase
                    var phrase = current.ToString();
                    if (phrase.Length > 0)
                    {
                        if (isNegated)
                            negated.Add(phrase);
                        else
                            phrases.Add(phrase);
                    }
                    current.Clear();
                    isNegated = false;
                    inQuote = false;
                }
                else
                {
                    inQuote = true;
                }
            }
            else if (c == ' ' && !inQuote)
            {
                if (current.Length > 0)
                {
                    var word = current.ToString();
                    if (isNegated)
                        negated.Add(word);
                    else
                        terms.Add(word);
                    current.Clear();
                    isNegated = false;
                }
            }
            else if (c == '-' && current.Length == 0 && !inQuote)
            {
                isNegated = true;
            }
            else
            {
                current.Append(c);
            }
        }

        if (current.Length > 0)
        {
            var word = current.ToString();
            if (isNegated)
                negated.Add(word);
            else
                terms.Add(word);
        }

        return (terms, phrases, negated);
    }

    #region $jsonSchema

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/jsonSchema/
    //   "The $jsonSchema operator matches documents that satisfy the specified JSON Schema."
    private static bool MatchesJsonSchema(BsonDocument document, BsonDocument schema)
    {
        // Check 'required' — array of field names that must be present
        if (schema.Contains("required") && schema["required"].IsBsonArray)
        {
            foreach (var field in schema["required"].AsBsonArray)
            {
                if (!document.Contains(field.AsString))
                    return false;
            }
        }

        // Check 'bsonType' — the document-level BSON type constraint
        if (schema.Contains("bsonType"))
        {
            var expectedType = schema["bsonType"].AsString;
            if (expectedType == "object" && document.BsonType != BsonType.Document)
                return false;
        }

        // Check 'properties' — per-field schema constraints
        if (schema.Contains("properties") && schema["properties"].IsBsonDocument)
        {
            foreach (var prop in schema["properties"].AsBsonDocument)
            {
                if (!document.Contains(prop.Name)) continue;
                if (!MatchesPropertySchema(document[prop.Name], prop.Value.AsBsonDocument))
                    return false;
            }
        }

        // Check 'additionalProperties: false'
        if (schema.Contains("additionalProperties") && schema["additionalProperties"] == false)
        {
            var allowedFields = new HashSet<string> { "_id" };
            if (schema.Contains("properties"))
            {
                foreach (var prop in schema["properties"].AsBsonDocument)
                    allowedFields.Add(prop.Name);
            }
            foreach (var field in document)
            {
                if (!allowedFields.Contains(field.Name))
                    return false;
            }
        }

        // Check 'minProperties' and 'maxProperties'
        if (schema.Contains("minProperties") && document.ElementCount < schema["minProperties"].ToInt32())
            return false;
        if (schema.Contains("maxProperties") && document.ElementCount > schema["maxProperties"].ToInt32())
            return false;

        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/jsonSchema/
        //   "allOf: Must match ALL of the given schemas."
        if (schema.Contains("allOf") && schema["allOf"].IsBsonArray)
        {
            foreach (var subSchema in schema["allOf"].AsBsonArray)
            {
                if (!MatchesJsonSchema(document, subSchema.AsBsonDocument))
                    return false;
            }
        }

        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/jsonSchema/
        //   "anyOf: Must match at least ONE of the given schemas."
        if (schema.Contains("anyOf") && schema["anyOf"].IsBsonArray)
        {
            var matched = schema["anyOf"].AsBsonArray.Any(s => MatchesJsonSchema(document, s.AsBsonDocument));
            if (!matched)
                return false;
        }

        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/jsonSchema/
        //   "oneOf: Must match exactly ONE of the given schemas."
        if (schema.Contains("oneOf") && schema["oneOf"].IsBsonArray)
        {
            var matchCount = schema["oneOf"].AsBsonArray.Count(s => MatchesJsonSchema(document, s.AsBsonDocument));
            if (matchCount != 1)
                return false;
        }

        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/jsonSchema/
        //   "not: Must NOT match the given schema."
        if (schema.Contains("not") && schema["not"].IsBsonDocument)
        {
            if (MatchesJsonSchema(document, schema["not"].AsBsonDocument))
                return false;
        }

        return true;
    }

    private static bool MatchesPropertySchema(BsonValue value, BsonDocument propSchema)
    {
        // bsonType
        if (propSchema.Contains("bsonType"))
        {
            var expected = propSchema["bsonType"];
            if (expected.IsString)
            {
                if (!BsonTypeMatches(value, expected.AsString))
                    return false;
            }
            else if (expected.IsBsonArray)
            {
                if (!expected.AsBsonArray.Any(t => BsonTypeMatches(value, t.AsString)))
                    return false;
            }
        }

        // enum
        if (propSchema.Contains("enum"))
        {
            if (!propSchema["enum"].AsBsonArray.Contains(value))
                return false;
        }

        // Numeric constraints
        if (value.IsNumeric)
        {
            var numVal = value.ToDouble();
            if (propSchema.Contains("minimum") && numVal < propSchema["minimum"].ToDouble())
                return false;
            if (propSchema.Contains("maximum") && numVal > propSchema["maximum"].ToDouble())
                return false;
            if (propSchema.Contains("exclusiveMinimum") && numVal <= propSchema["exclusiveMinimum"].ToDouble())
                return false;
            if (propSchema.Contains("exclusiveMaximum") && numVal >= propSchema["exclusiveMaximum"].ToDouble())
                return false;
        }

        // String constraints
        if (value.IsString)
        {
            var str = value.AsString;
            if (propSchema.Contains("minLength") && str.Length < propSchema["minLength"].ToInt32())
                return false;
            if (propSchema.Contains("maxLength") && str.Length > propSchema["maxLength"].ToInt32())
                return false;
            if (propSchema.Contains("pattern"))
            {
                var regex = new System.Text.RegularExpressions.Regex(propSchema["pattern"].AsString);
                if (!regex.IsMatch(str))
                    return false;
            }
        }

        // Array constraints
        if (value.IsBsonArray)
        {
            var arr = value.AsBsonArray;
            if (propSchema.Contains("minItems") && arr.Count < propSchema["minItems"].ToInt32())
                return false;
            if (propSchema.Contains("maxItems") && arr.Count > propSchema["maxItems"].ToInt32())
                return false;
            if (propSchema.Contains("uniqueItems") && propSchema["uniqueItems"].AsBoolean)
            {
                if (arr.Distinct(BsonValueComparer.Instance).Count() != arr.Count)
                    return false;
            }
        }

        // Nested object
        if (value.IsBsonDocument && propSchema.Contains("properties"))
        {
            return MatchesJsonSchema(value.AsBsonDocument, propSchema);
        }

        return true;
    }

    private static bool BsonTypeMatches(BsonValue value, string typeName)
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/jsonSchema/#available-keywords
        return typeName switch
        {
            "double" => value.BsonType == BsonType.Double,
            "string" => value.BsonType == BsonType.String,
            "object" => value.BsonType == BsonType.Document,
            "array" => value.BsonType == BsonType.Array,
            "binData" => value.BsonType == BsonType.Binary,
            "objectId" => value.BsonType == BsonType.ObjectId,
            "bool" => value.BsonType == BsonType.Boolean,
            "date" => value.BsonType == BsonType.DateTime,
            "null" => value.BsonType == BsonType.Null,
            "regex" => value.BsonType == BsonType.RegularExpression,
            "int" => value.BsonType == BsonType.Int32,
            "long" => value.BsonType == BsonType.Int64,
            "decimal" => value.BsonType == BsonType.Decimal128,
            "timestamp" => value.BsonType == BsonType.Timestamp,
            "number" => value.IsNumeric,
            _ => false
        };
    }

    #endregion

    #region $where

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/where/
    //   "Use the $where operator to pass either a string containing a JavaScript expression
    //    or a full JavaScript function to the query system."
    //   "Map the specified JavaScript function, which can contain a full function or just
    //    the body of a function."
    // Note: Actual JavaScript execution requires the JsTriggers package.
    // Without JsTriggers, $where throws NotSupportedException.
    private static Func<BsonDocument, BsonValue, bool>? _whereEvaluator;

    /// <summary>
    /// Registers a custom $where evaluator (typically from the JsTriggers package).
    /// </summary>
    internal static void RegisterWhereEvaluator(Func<BsonDocument, BsonValue, bool> evaluator)
    {
        _whereEvaluator = evaluator;
    }

    private static bool EvaluateWhere(BsonDocument document, BsonValue whereExpr)
    {
        if (_whereEvaluator != null)
            return _whereEvaluator(document, whereExpr);

        throw new NotSupportedException(
            "$where requires the InMemoryEmulator.MongoDB.JsTriggers package for JavaScript execution. " +
            "Install the package and call JsExpressionSetup.Register() to enable $where support.");
    }

    #endregion
}
