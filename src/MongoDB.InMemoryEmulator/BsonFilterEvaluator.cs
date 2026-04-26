using MongoDB.Bson;

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
}
