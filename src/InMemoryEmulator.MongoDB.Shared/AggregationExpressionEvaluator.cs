using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using global::MongoDB.Bson;

namespace InMemoryEmulator.MongoDB;

/// <summary>
/// Evaluates MongoDB aggregation expressions ($add, $concat, $cond, field references, etc.).
/// Used by $project, $addFields, $group accumulators, $match ($expr), and pipeline updates.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/meta/aggregation-quick-reference/#expressions
///   "Expressions can include field paths, literals, system variables, expression objects, and expression operators."
/// </remarks>
internal static class AggregationExpressionEvaluator
{
    /// <summary>
    /// Sentinel value for $$REMOVE — indicates the field should be excluded from the output.
    /// Distinguished from BsonNull.Value because null fields are included, but $$REMOVE fields are omitted.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/aggregation-variables/
    ///   "$$REMOVE evaluates to the missing value. Allows for the exclusion of fields
    ///    in $addFields and $project stages."
    /// </remarks>
    internal static readonly BsonValue RemoveSentinel = new BsonString("$$__REMOVE_SENTINEL__$$");

    /// <summary>
    /// Returns true if the value is the $$REMOVE sentinel.
    /// </summary>
    internal static bool IsRemove(BsonValue value) => ReferenceEquals(value, RemoveSentinel);

    /// <summary>
    /// Evaluate an expression, but return RemoveSentinel when a simple field path
    /// refers to a missing field (as opposed to an explicitly null field).
    /// Used by $push/$addToSet accumulators which skip missing fields.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/push/
    ///   Real MongoDB $push does not include a value for documents where the field is missing.
    /// </remarks>
    internal static BsonValue EvaluateSkipMissing(BsonDocument doc, BsonValue expr, BsonDocument? variables = null)
    {
        if (expr is BsonString s && s.Value.StartsWith("$") && !s.Value.StartsWith("$$"))
        {
            var fieldPath = s.Value[1..];
            if (!BsonFilterEvaluator.FieldExists(doc, fieldPath))
                return RemoveSentinel;
        }
        return Evaluate(doc, expr, variables);
    }

    /// <summary>
    /// Evaluate an expression against a document, returning the resulting BsonValue.
    /// </summary>
    internal static BsonValue Evaluate(BsonDocument doc, BsonValue expr, BsonDocument? variables = null)
    {
        // Ref: https://www.mongodb.com/docs/manual/meta/aggregation-quick-reference/#expressions
        //   "Field path: $fieldName or $nested.field"
        if (expr is BsonString s)
        {
            if (s.Value.StartsWith("$$"))
            {
                var varName = s.Value[2..];
                return ResolveVariable(varName, doc, variables);
            }
            if (s.Value.StartsWith("$"))
            {
                var fieldPath = s.Value[1..];
                return BsonFilterEvaluator.ResolveFieldPath(doc, fieldPath);
            }
            return expr; // literal string
        }

        // Literal values pass through
        if (expr is not BsonDocument exprDoc)
            return expr;

        // Empty doc is literal
        if (exprDoc.ElementCount == 0)
            return exprDoc;

        // Check if it's an operator expression (single key starting with $)
        var firstName = exprDoc.GetElement(0).Name;
        if (!firstName.StartsWith("$"))
        {
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/group/
            //   "You can specify an _id value of a document that contains field path expressions."
            // Non-operator documents may contain expressions in their values (e.g., { year: "$year" })
            // Recursively evaluate each value.
            var result = new BsonDocument();
            foreach (var el in exprDoc)
            {
                result[el.Name] = Evaluate(doc, el.Value, variables);
            }
            return result;
        }

        if (exprDoc.ElementCount == 1)
        {
            var args = exprDoc[firstName];
            return EvaluateOperator(doc, firstName, args, variables);
        }

        // Multi-key document with $-keys could be $cond shorthand or literal
        // $cond: { if: ..., then: ..., else: ... }
        if (exprDoc.Contains("$cond"))
            return EvaluateOperator(doc, "$cond", exprDoc["$cond"], variables);

        return exprDoc;
    }

    private static BsonValue EvaluateOperator(BsonDocument doc, string op, BsonValue args, BsonDocument? variables)
    {
        return op switch
        {
            // Arithmetic
            "$add" => EvalAdd(doc, args, variables),
            "$subtract" => EvalSubtract(doc, args, variables),
            "$multiply" => EvalMultiply(doc, args, variables),
            "$divide" => EvalDivide(doc, args, variables),
            "$mod" => EvalMod(doc, args, variables),
            "$abs" => EvalUnaryMathTypePreserving(doc, args, variables, Math.Abs),
            "$ceil" => EvalUnaryMathTypePreserving(doc, args, variables, Math.Ceiling),
            "$floor" => EvalUnaryMathTypePreserving(doc, args, variables, Math.Floor),
            "$round" => EvalRound(doc, args, variables),
            "$trunc" => EvalTrunc(doc, args, variables),
            "$pow" => EvalBinaryMath(doc, args, variables, Math.Pow),
            "$sqrt" => EvalUnaryMath(doc, args, variables, Math.Sqrt),
            "$log" => EvalBinaryMath(doc, args, variables, Math.Log),
            "$log10" => EvalUnaryMath(doc, args, variables, Math.Log10),
            "$ln" => EvalUnaryMath(doc, args, variables, Math.Log),
            "$exp" => EvalUnaryMath(doc, args, variables, Math.Exp),

            // Trig
            "$sin" => EvalUnaryMath(doc, args, variables, Math.Sin),
            "$cos" => EvalUnaryMath(doc, args, variables, Math.Cos),
            "$tan" => EvalUnaryMath(doc, args, variables, Math.Tan),
            "$asin" => EvalUnaryMath(doc, args, variables, Math.Asin),
            "$acos" => EvalUnaryMath(doc, args, variables, Math.Acos),
            "$atan" => EvalUnaryMath(doc, args, variables, Math.Atan),
            "$atan2" => EvalBinaryMath(doc, args, variables, Math.Atan2),
            "$degreesToRadians" => EvalUnaryMath(doc, args, variables, d => d * Math.PI / 180.0),
            "$radiansToDegrees" => EvalUnaryMath(doc, args, variables, d => d * 180.0 / Math.PI),

            // String
            "$concat" => EvalConcat(doc, args, variables),
            "$toLower" => EvalStringUnary(doc, args, variables, s => s.ToLowerInvariant()),
            "$toUpper" => EvalStringUnary(doc, args, variables, s => s.ToUpperInvariant()),
            "$trim" => EvalTrim(doc, args, variables, s => s.Trim(), (s, c) => s.Trim(c)),
            "$ltrim" => EvalTrim(doc, args, variables, s => s.TrimStart(), (s, c) => s.TrimStart(c)),
            "$rtrim" => EvalTrim(doc, args, variables, s => s.TrimEnd(), (s, c) => s.TrimEnd(c)),
            "$substr" or "$substrBytes" => EvalSubstr(doc, args, variables),
            "$substrCP" => EvalSubstr(doc, args, variables),
            "$strLenBytes" => EvalStrLen(doc, args, variables, s => Encoding.UTF8.GetByteCount(s)),
            "$strLenCP" => EvalStrLen(doc, args, variables, s => s.Length),
            "$indexOfBytes" or "$indexOfCP" => EvalIndexOf(doc, args, variables),
            "$split" => EvalSplit(doc, args, variables),
            "$strcasecmp" => EvalStrcasecmp(doc, args, variables),
            "$replaceOne" => EvalReplaceOne(doc, args, variables),
            "$replaceAll" => EvalReplaceAll(doc, args, variables),
            "$regexMatch" => EvalRegexMatch(doc, args, variables),
            "$regexFind" => EvalRegexFind(doc, args, variables),
            "$regexFindAll" => EvalRegexFindAll(doc, args, variables),

            // Comparison
            "$cmp" => EvalCmp(doc, args, variables),
            "$eq" => EvalCompare(doc, args, variables, r => r == 0),
            "$ne" => EvalCompare(doc, args, variables, r => r != 0),
            "$gt" => EvalCompare(doc, args, variables, r => r > 0),
            "$gte" => EvalCompare(doc, args, variables, r => r >= 0),
            "$lt" => EvalCompare(doc, args, variables, r => r < 0),
            "$lte" => EvalCompare(doc, args, variables, r => r <= 0),

            // Conditional
            "$cond" => EvalCond(doc, args, variables),
            "$ifNull" => EvalIfNull(doc, args, variables),
            "$switch" => EvalSwitch(doc, args, variables),

            // Boolean
            "$and" => EvalAnd(doc, args, variables),
            "$or" => EvalOr(doc, args, variables),
            "$not" => EvalNot(doc, args, variables),

            // Array
            "$arrayElemAt" => EvalArrayElemAt(doc, args, variables),
            "$size" => EvalSize(doc, args, variables),
            "$isArray" => EvalIsArray(doc, args, variables),
            "$concatArrays" => EvalConcatArrays(doc, args, variables),
            "$in" => EvalIn(doc, args, variables),
            "$filter" => EvalFilter(doc, args, variables),
            "$map" => EvalMap(doc, args, variables),
            "$reduce" => EvalReduce(doc, args, variables),
            "$reverseArray" => EvalReverseArray(doc, args, variables),
            "$range" => EvalRange(doc, args, variables),
            "$slice" => EvalSlice(doc, args, variables),
            "$first" => EvalFirst(doc, args, variables),
            "$last" => EvalLast(doc, args, variables),
            "$sortArray" => EvalSortArray(doc, args, variables),
            "$objectToArray" => EvalObjectToArray(doc, args, variables),
            "$arrayToObject" => EvalArrayToObject(doc, args, variables),
            "$zip" => EvalZip(doc, args, variables),
            "$indexOfArray" => EvalIndexOfArray(doc, args, variables),

            // Set operators
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/#set-expression-operators
            "$setUnion" => EvalSetUnion(doc, args, variables),
            "$setIntersection" => EvalSetIntersection(doc, args, variables),
            "$setDifference" => EvalSetDifference(doc, args, variables),
            "$setEquals" => EvalSetEquals(doc, args, variables),
            "$setIsSubset" => EvalSetIsSubset(doc, args, variables),
            "$anyElementTrue" => EvalAnyElementTrue(doc, args, variables),
            "$allElementsTrue" => EvalAllElementsTrue(doc, args, variables),

            // Type
            "$type" => EvalType(doc, args, variables),
            "$convert" => EvalConvert(doc, args, variables),
            "$toBool" => EvalToBool(doc, args, variables),
            "$toInt" => EvalToInt(doc, args, variables),
            "$toLong" => EvalToLong(doc, args, variables),
            "$toDouble" => EvalToDouble(doc, args, variables),
            "$toDecimal" => EvalToDecimal(doc, args, variables),
            "$toString" => EvalToString(doc, args, variables),
            "$toDate" => EvalToDate(doc, args, variables),
            "$toObjectId" => EvalToObjectId(doc, args, variables),
            "$isNumber" => EvalIsNumber(doc, args, variables),

            // Date
            "$year" => EvalDatePart(doc, args, variables, d => d.Year),
            "$month" => EvalDatePart(doc, args, variables, d => d.Month),
            "$dayOfMonth" => EvalDatePart(doc, args, variables, d => d.Day),
            "$hour" => EvalDatePart(doc, args, variables, d => d.Hour),
            "$minute" => EvalDatePart(doc, args, variables, d => d.Minute),
            "$second" => EvalDatePart(doc, args, variables, d => d.Second),
            "$millisecond" => EvalDatePart(doc, args, variables, d => d.Millisecond),
            "$dayOfWeek" => EvalDatePart(doc, args, variables, d => (int)d.DayOfWeek + 1),
            "$dayOfYear" => EvalDatePart(doc, args, variables, d => d.DayOfYear),
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/week/
            //   "Returns the week of the year for a date as a number between 0 and 53."
            //   "Weeks begin on Sundays, and week 1 begins with the first Sunday of the year."
            "$week" => EvalDatePart(doc, args, variables, d =>
            {
                // Days preceding the first Sunday of the year are in week 0.
                var jan1 = new DateTime(d.Year, 1, 1);
                var firstSunday = jan1.AddDays((7 - (int)jan1.DayOfWeek) % 7);
                if (d < firstSunday) return 0;
                return (int)((d - firstSunday).TotalDays / 7) + 1;
            }),
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/isoDayOfWeek/
            //   "Returns the weekday number in ISO 8601 format, ranging from 1 (Monday) to 7 (Sunday)."
            "$isoDayOfWeek" => EvalDatePart(doc, args, variables, d =>
                d.DayOfWeek == DayOfWeek.Sunday ? 7 : (int)d.DayOfWeek),
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/isoWeek/
            //   "Returns the week number in ISO 8601 format, ranging from 1 to 53."
            "$isoWeek" => EvalDatePart(doc, args, variables, d =>
                System.Globalization.ISOWeek.GetWeekOfYear(d)),
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/isoWeekYear/
            //   "Returns the year number in ISO 8601 format."
            "$isoWeekYear" => EvalDatePart(doc, args, variables, d =>
                System.Globalization.ISOWeek.GetYear(d)),
            "$dateToString" => EvalDateToString(doc, args, variables),
            "$dateFromString" => EvalDateFromString(doc, args, variables),
            "$dateAdd" => EvalDateAdd(doc, args, variables),
            "$dateSubtract" => EvalDateSubtract(doc, args, variables),
            "$dateDiff" => EvalDateDiff(doc, args, variables),
            "$dateTrunc" => EvalDateTrunc(doc, args, variables),
            "$dateFromParts" => EvalDateFromParts(doc, args, variables),
            "$dateToParts" => EvalDateToParts(doc, args, variables),

            // Object
            "$mergeObjects" => EvalMergeObjects(doc, args, variables),
            "$getField" => EvalGetField(doc, args, variables),
            "$setField" => EvalSetField(doc, args, variables),
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/unsetField/
            //   "$unsetField is an alias for $setField with value: $$REMOVE"
            "$unsetField" => EvalUnsetField(doc, args, variables),

            // Literal
            "$literal" => args,

            // Let
            "$let" => EvalLet(doc, args, variables),

            // Bitwise
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/bitAnd/
            "$bitAnd" => EvalBitwiseAggregate(doc, args, variables, (a, b) => a & b),
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/bitOr/
            "$bitOr" => EvalBitwiseAggregate(doc, args, variables, (a, b) => a | b),
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/bitXor/
            "$bitXor" => EvalBitwiseAggregate(doc, args, variables, (a, b) => a ^ b),
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/bitNot/
            "$bitNot" => EvalBitNot(doc, args, variables),

            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/binarySize/
            //   "Returns the size in bytes of a given string or binary data value."
            "$binarySize" => EvalBinarySize(doc, args, variables),
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/bsonSize/
            //   "Returns the size in bytes of a given document when encoded as BSON."
            "$bsonSize" => EvalBsonSize(doc, args, variables),
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/rand/
            //   "Returns a random float between 0 and 1."
            "$rand" => new BsonDouble(Random.Shared.NextDouble()),
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/sampleRate/
            //   "Matches a random selection of input documents."
            "$sampleRate" => new BsonBoolean(Random.Shared.NextDouble() < Evaluate(doc, args, variables).ToDouble()),
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/toHashedIndexKey/
            //   "Computes and returns the hash value of the input expression using the same hash function."
            "$toHashedIndexKey" => EvalToHashedIndexKey(doc, args, variables),
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/tsIncrement/
            //   "Returns the incrementing ordinal from a timestamp as a long."
            "$tsIncrement" => EvalTsIncrement(doc, args, variables),
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/tsSecond/
            //   "Returns the seconds from a timestamp as a long."
            "$tsSecond" => EvalTsSecond(doc, args, variables),

            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/function/
            //   "Defines a custom aggregation function or expression in JavaScript."
            "$function" => EvalFunction(doc, args, variables),

            _ => throw new NotSupportedException($"Expression operator '{op}' is not supported.")
        };
    }

    #region Variable Resolution

    private static BsonValue ResolveVariable(string name, BsonDocument doc, BsonDocument? variables)
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/aggregation-variables/
        //   "$$variable.field.path" — resolve variable then traverse field path

        // Handle dotted paths on variables: $$item.qty -> resolve "item", then field path "qty"
        var dotIndex = name.IndexOf('.');
        var varPart = dotIndex >= 0 ? name[..dotIndex] : name;
        var fieldPart = dotIndex >= 0 ? name[(dotIndex + 1)..] : null;

        var value = varPart switch
        {
            "ROOT" => doc,
            "CURRENT" => doc,
            "NOW" => new BsonDateTime(DateTime.UtcNow),
            "CLUSTER_TIME" => new BsonTimestamp((int)DateTimeOffset.UtcNow.ToUnixTimeSeconds(), 1),
            "REMOVE" => RemoveSentinel, // Special sentinel — must be distinguished from null
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/redact/
            //   "$$KEEP, $$PRUNE, $$DESCEND are system variables used by $redact."
            "KEEP" => new BsonString("$$KEEP"),
            "PRUNE" => new BsonString("$$PRUNE"),
            "DESCEND" => new BsonString("$$DESCEND"),
            _ => variables != null && variables.Contains(varPart) ? variables[varPart] : BsonNull.Value,
        };

        // If there's a field path after the variable, resolve it
        if (fieldPart != null && value is BsonDocument valueDoc)
            return BsonFilterEvaluator.ResolveFieldPath(valueDoc, fieldPart);

        return value;
    }

    #endregion

    #region Arithmetic

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/add/
    //   "If one of the arguments is a date, $add treats the other arguments as milliseconds
    //    to add to the date."
    private static BsonValue EvalAdd(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arr = EvalArray(doc, args, variables);

        // Check if any argument is a date
        BsonDateTime? dateArg = null;
        decimal decimalSum = 0;
        double numericSum = 0;
        bool hasDecimal = false;
        bool hasDouble = false;
        bool hasLong = false;
        foreach (var v in arr)
        {
            if (v == BsonNull.Value) return BsonNull.Value;
            if (v.BsonType == BsonType.DateTime)
            {
                if (dateArg != null)
                    throw MongoErrors.BadValue("only one date allowed in an $add expression");
                dateArg = v.AsBsonDateTime;
            }
            else
            {
                numericSum += v.ToDouble();
                if (v.BsonType == BsonType.Decimal128) { hasDecimal = true; decimalSum += v.AsDecimal; }
                else if (v.BsonType == BsonType.Double) { hasDouble = true; decimalSum += (decimal)v.AsDouble; }
                else { var lv = v.ToInt64(); decimalSum += lv; if (v.BsonType == BsonType.Int64) hasLong = true; }
            }
        }

        if (dateArg != null)
            return new BsonDateTime(dateArg.ToUniversalTime().AddMilliseconds(numericSum));

        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/add/
        //   Type promotion: integer → long → double → decimal
        if (hasDecimal) return new BsonDecimal128((Decimal128)decimalSum);
        return WrapNumeric(numericSum, hasDouble, hasLong);
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/subtract/
    //   "If the two values are dates, return the difference in milliseconds."
    //   "If the two values are a date and a number... subtracts the number, in milliseconds, from the date."
    private static BsonValue EvalSubtract(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arr = EvalArray(doc, args, variables);
        if (arr[0] == BsonNull.Value || arr[1] == BsonNull.Value) return BsonNull.Value;

        bool firstIsDate = arr[0].BsonType == BsonType.DateTime;
        bool secondIsDate = arr[1].BsonType == BsonType.DateTime;

        if (firstIsDate && secondIsDate)
        {
            // Date - Date = difference in milliseconds
            var ms = (arr[0].AsBsonDateTime.ToUniversalTime() - arr[1].AsBsonDateTime.ToUniversalTime()).TotalMilliseconds;
            return new BsonInt64((long)ms);
        }

        if (firstIsDate)
        {
            // Date - number = Date minus milliseconds
            return new BsonDateTime(arr[0].AsBsonDateTime.ToUniversalTime().AddMilliseconds(-arr[1].ToDouble()));
        }

        bool hasDecimal = arr[0].BsonType == BsonType.Decimal128 || arr[1].BsonType == BsonType.Decimal128;
        bool hasDouble = arr[0].BsonType == BsonType.Double || arr[1].BsonType == BsonType.Double;
        bool hasLong = arr[0].BsonType == BsonType.Int64 || arr[1].BsonType == BsonType.Int64;
        if (hasDecimal)
        {
            decimal a = arr[0].BsonType == BsonType.Decimal128 ? arr[0].AsDecimal : (decimal)arr[0].ToDouble();
            decimal b = arr[1].BsonType == BsonType.Decimal128 ? arr[1].AsDecimal : (decimal)arr[1].ToDouble();
            return new BsonDecimal128((Decimal128)(a - b));
        }
        return WrapNumeric(arr[0].ToDouble() - arr[1].ToDouble(), hasDouble, hasLong);
    }

    private static BsonValue EvalMultiply(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/multiply/
        //   Type promotion: integer → long → double → decimal
        var arr = EvalArray(doc, args, variables);
        double product = 1;
        decimal decProduct = 1;
        bool hasDecimal = false;
        bool hasDouble = false;
        bool hasLong = false;
        foreach (var v in arr)
        {
            if (v == BsonNull.Value) return BsonNull.Value;
            product *= v.ToDouble();
            if (v.BsonType == BsonType.Decimal128) { hasDecimal = true; decProduct *= v.AsDecimal; }
            else if (v.BsonType == BsonType.Double) { hasDouble = true; decProduct *= (decimal)v.AsDouble; }
            else { var lv = v.ToInt64(); decProduct *= lv; if (v.BsonType == BsonType.Int64) hasLong = true; }
        }
        if (hasDecimal) return new BsonDecimal128((Decimal128)decProduct);
        return WrapNumeric(product, hasDouble, hasLong);
    }

    private static BsonValue EvalDivide(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arr = EvalArray(doc, args, variables);
        if (arr[0] == BsonNull.Value || arr[1] == BsonNull.Value) return BsonNull.Value;
        var divisor = arr[1].ToDouble();
        if (divisor == 0) throw MongoErrors.BadValue("can't $divide by zero");
        return new BsonDouble(arr[0].ToDouble() / divisor);
    }

    private static BsonValue EvalMod(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/mod/
        //   Type promotion: integer → long → double → decimal
        var arr = EvalArray(doc, args, variables);
        if (arr[0] == BsonNull.Value || arr[1] == BsonNull.Value) return BsonNull.Value;
        bool hasDecimal = arr[0].BsonType == BsonType.Decimal128 || arr[1].BsonType == BsonType.Decimal128;
        bool hasDouble = arr[0].BsonType == BsonType.Double || arr[1].BsonType == BsonType.Double;
        bool hasLong = arr[0].BsonType == BsonType.Int64 || arr[1].BsonType == BsonType.Int64;
        if (hasDecimal)
        {
            decimal a = arr[0].BsonType == BsonType.Decimal128 ? arr[0].AsDecimal : (decimal)arr[0].ToDouble();
            decimal b = arr[1].BsonType == BsonType.Decimal128 ? arr[1].AsDecimal : (decimal)arr[1].ToDouble();
            return new BsonDecimal128((Decimal128)(a % b));
        }
        return WrapNumeric(arr[0].ToDouble() % arr[1].ToDouble(), hasDouble, hasLong);
    }

    /// <summary>
    /// Wraps a numeric result in the appropriate BsonValue type based on input type promotion rules.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/add/
    ///   "Type promotion: integer → long → double → decimal"
    /// </remarks>
    private static BsonValue WrapNumeric(double value, bool hasDouble, bool hasLong)
    {
        if (hasDouble) return new BsonDouble(value);
        if (hasLong) return new BsonInt64((long)value);
        return new BsonInt32((int)value);
    }

    private static BsonValue EvalUnaryMath(BsonDocument doc, BsonValue args, BsonDocument? variables, Func<double, double> fn)
    {
        var val = Evaluate(doc, args is BsonArray a ? a[0] : args, variables);
        if (val == BsonNull.Value) return BsonNull.Value;
        return new BsonDouble(fn(val.ToDouble()));
    }

    /// <summary>
    /// Evaluates a unary math operator that preserves the input numeric type.
    /// Used for $abs, $ceil, $floor, $trunc.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/abs/
    ///   "Returns a value with the same type as the input value."
    /// </remarks>
    private static BsonValue EvalUnaryMathTypePreserving(BsonDocument doc, BsonValue args, BsonDocument? variables, Func<double, double> fn)
    {
        var val = Evaluate(doc, args is BsonArray a ? a[0] : args, variables);
        if (val == BsonNull.Value) return BsonNull.Value;
        var result = fn(val.ToDouble());
        return val.BsonType switch
        {
            BsonType.Int32 => new BsonInt32((int)result),
            BsonType.Int64 => new BsonInt64((long)result),
            BsonType.Decimal128 => new BsonDecimal128((Decimal128)(decimal)result),
            _ => new BsonDouble(result)
        };
    }

    private static BsonValue EvalBinaryMath(BsonDocument doc, BsonValue args, BsonDocument? variables, Func<double, double, double> fn)
    {
        var arr = EvalArray(doc, args, variables);
        if (arr[0] == BsonNull.Value || arr[1] == BsonNull.Value) return BsonNull.Value;
        return new BsonDouble(fn(arr[0].ToDouble(), arr[1].ToDouble()));
    }

    private static BsonValue EvalRound(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arr = EvalArray(doc, args, variables);
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/round/
        //   "If the argument resolves to a value of null or refers to a missing field, $round returns null."
        //   "$round returns a value with the same type as the input value."
        if (arr[0] == BsonNull.Value) return BsonNull.Value;
        int places = arr.Count > 1 ? arr[1].ToInt32() : 0;

        var input = arr[0];
        if (input.BsonType == BsonType.Int32)
        {
            if (places >= 0) return input;
            double factor = Math.Pow(10, places);
            return new BsonInt32((int)(Math.Round(input.AsInt32 * factor, MidpointRounding.ToEven) / factor));
        }
        if (input.BsonType == BsonType.Int64)
        {
            if (places >= 0) return input;
            double factor = Math.Pow(10, places);
            return new BsonInt64((long)(Math.Round(input.AsInt64 * factor, MidpointRounding.ToEven) / factor));
        }
        if (input.BsonType == BsonType.Decimal128)
        {
            var dec = input.AsDecimal;
            if (places >= 0)
                return new BsonDecimal128(Math.Round(dec, places, MidpointRounding.ToEven));
            var factor = (decimal)Math.Pow(10, places);
            return new BsonDecimal128(Math.Round(dec * factor, MidpointRounding.ToEven) / factor);
        }

        //   "Rounds using the IEEE 754 round-to-even rule."
        var val = input.ToDouble();
        if (places >= 0)
            return new BsonDouble(Math.Round(val, places, MidpointRounding.ToEven));
        double f = Math.Pow(10, places);
        return new BsonDouble(Math.Round(val * f, MidpointRounding.ToEven) / f);
    }

    private static BsonValue EvalTrunc(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arr = EvalArray(doc, args, variables);
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/trunc/
        //   "If the argument resolves to a value of null or refers to a missing field, $trunc returns null."
        //   "$trunc returns a value with the same type as the input value."
        if (arr[0] == BsonNull.Value) return BsonNull.Value;
        int places = arr.Count > 1 ? arr[1].ToInt32() : 0;

        var input = arr[0];
        if (input.BsonType == BsonType.Int32)
        {
            if (places >= 0) return input;
            double factor = Math.Pow(10, places);
            return new BsonInt32((int)(Math.Truncate(input.AsInt32 * factor) / factor));
        }
        if (input.BsonType == BsonType.Int64)
        {
            if (places >= 0) return input;
            double factor = Math.Pow(10, places);
            return new BsonInt64((long)(Math.Truncate(input.AsInt64 * factor) / factor));
        }
        if (input.BsonType == BsonType.Decimal128)
        {
            var dec = input.AsDecimal;
            var factor = (decimal)Math.Pow(10, places);
            return new BsonDecimal128(Math.Truncate(dec * factor) / factor);
        }

        var val = input.ToDouble();
        double f = Math.Pow(10, places);
        return new BsonDouble(Math.Truncate(val * f) / f);
    }

    #endregion

    #region String

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/concat/
    private static BsonValue EvalConcat(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arr = EvalArray(doc, args, variables);
        var sb = new StringBuilder();
        foreach (var v in arr)
        {
            if (v == BsonNull.Value) return BsonNull.Value;
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/concat/
            //   "$concat only supports strings, not <type>"
            if (!v.IsString)
                throw MongoErrors.BadValue($"$concat only supports strings, not {v.BsonType}");
            sb.Append(v.AsString);
        }
        return new BsonString(sb.ToString());
    }

    private static BsonValue EvalStringUnary(BsonDocument doc, BsonValue args, BsonDocument? variables, Func<string, string> fn)
    {
        var val = Evaluate(doc, args is BsonArray a ? a[0] : args, variables);
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/toUpper/
        //   "If the argument resolves to null, $toUpper returns an empty string ''." (same for $toLower)
        if (val == BsonNull.Value) return new BsonString("");
        if (val.IsString) return new BsonString(fn(val.AsString));
        // Ref: Observed real MongoDB 7.0 behavior:
        //   Numeric types (int32, int64, double, decimal128) are converted to their string representation.
        //   Non-numeric, non-string types (bool, array, etc.) throw an error.
        if (val.IsNumeric || val.BsonType == BsonType.Decimal128)
            return new BsonString(fn(val.ToString()!));
        throw MongoErrors.BadValue($"can't convert from BSON type {val.BsonType} to String");
    }

    private static BsonValue EvalTrim(BsonDocument doc, BsonValue args, BsonDocument? variables,
        Func<string, string> fn, Func<string, char[], string> fnWithChars)
    {
        if (args.IsBsonDocument)
        {
            var spec = args.AsBsonDocument;
            var input = Evaluate(doc, spec["input"], variables);
            if (input == BsonNull.Value) return BsonNull.Value;
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/trim/
            //   "$trim requires its input to be a string"
            if (!input.IsString)
                throw MongoErrors.BadValue($"$trim requires its input to be a string, found: {input.BsonType}");
            var str = input.AsString;
            if (spec.Contains("chars"))
            {
                // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/ltrim/
                //   "$ltrim removes characters from the beginning of a string."
                // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/rtrim/
                //   "$rtrim removes characters from the end of a string."
                // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/trim/
                //   "If chars resolves to null, $trim returns null."
                var charsVal = Evaluate(doc, spec["chars"], variables);
                if (charsVal == BsonNull.Value) return BsonNull.Value;
                var chars = charsVal.AsString.ToCharArray();
                return new BsonString(fnWithChars(str, chars));
            }
            return new BsonString(fn(str));
        }
        var val = Evaluate(doc, args, variables);
        if (val == BsonNull.Value) return BsonNull.Value;
        if (!val.IsString)
            throw MongoErrors.BadValue($"$trim requires its input to be a string, found: {val.BsonType}");
        return new BsonString(fn(val.AsString));
    }

    private static BsonValue EvalSubstr(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arr = EvalArray(doc, args, variables);
        if (arr[0] == BsonNull.Value) return new BsonString("");
        var str = arr[0].AsString;
        var start = arr[1].ToInt32();
        var length = arr[2].ToInt32();
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/substrBytes/
        //   Real MongoDB 7.0 throws: "$substrBytes: starting index must be non-negative"
        if (start < 0)
            throw MongoErrors.BadValue("$substrBytes:  starting index must be non-negative (got: " + start + ")");
        if (start >= str.Length) return new BsonString("");
        return new BsonString(str.Substring(start, Math.Min(length, str.Length - start)));
    }

    private static BsonValue EvalStrLen(BsonDocument doc, BsonValue args, BsonDocument? variables, Func<string, int> fn)
    {
        var val = Evaluate(doc, args is BsonArray a ? a[0] : args, variables);
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/strLenBytes/
        //   "$strLenBytes requires a string argument, found: <type>"
        if (val == BsonNull.Value)
            throw MongoErrors.BadValue($"$strLenBytes requires a string argument, found: null");
        if (!val.IsString)
            throw MongoErrors.BadValue($"$strLenBytes requires a string argument, found: {val.BsonType}");
        return new BsonInt32(fn(val.AsString));
    }

    private static BsonValue EvalIndexOf(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arr = EvalArray(doc, args, variables);
        if (arr[0] == BsonNull.Value) return BsonNull.Value;
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/indexOfBytes/
        //   "Returns null if the first argument is null or missing."
        //   "<substring expression> can be any valid expression as long as it resolves to a string."
        if (arr[1] == BsonNull.Value)
            throw MongoErrors.BadValue("$indexOfBytes requires a string as the second argument, found: null");
        var str = arr[0].AsString;
        var sub = arr[1].AsString;
        int start = arr.Count > 2 ? arr[2].ToInt32() : 0;
        int end = arr.Count > 3 ? arr[3].ToInt32() : str.Length;
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/indexOfBytes/
        //   "If the <start> or <end> is a negative number, $indexOfBytes returns an error."
        if (start < 0)
            throw MongoErrors.BadValue("$indexOfBytes/$indexOfCP: starting index must be non-negative");
        if (end < 0)
            throw MongoErrors.BadValue("$indexOfBytes/$indexOfCP: ending index must be non-negative");
        if (start >= str.Length) return new BsonInt32(-1);
        end = Math.Min(end, str.Length);
        if (end <= start) return new BsonInt32(-1);
        var idx = str.IndexOf(sub, start, end - start, StringComparison.Ordinal);
        return new BsonInt32(idx);
    }

    private static BsonValue EvalSplit(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arr = EvalArray(doc, args, variables);
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/split/
        //   "Returns null if either argument is null."
        if (arr[0] == BsonNull.Value || arr[1] == BsonNull.Value) return BsonNull.Value;
        //   "Both arguments must be strings."
        if (!arr[0].IsString)
            throw MongoErrors.BadValue($"$split requires a string as the first argument, found: {arr[0].BsonType}");
        if (!arr[1].IsString)
            throw MongoErrors.BadValue($"$split requires a string as the second argument, found: {arr[1].BsonType}");
        var parts = arr[0].AsString.Split(arr[1].AsString);
        return new BsonArray(parts.Select(p => new BsonString(p)));
    }

    private static BsonValue EvalStrcasecmp(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arr = EvalArray(doc, args, variables);
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/strcasecmp/
        //   "Returns 1, 0, or -1."
        //   Null/missing values are treated as empty string for comparison.
        var s0 = arr[0] == BsonNull.Value ? "" : arr[0].AsString;
        var s1 = arr[1] == BsonNull.Value ? "" : arr[1].AsString;
        return new BsonInt32(Math.Sign(string.Compare(s0, s1, StringComparison.OrdinalIgnoreCase)));
    }

    private static BsonValue EvalReplaceOne(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var spec = args.AsBsonDocument;
        var input = Evaluate(doc, spec["input"], variables);
        if (input == BsonNull.Value) return BsonNull.Value;
        var find = Evaluate(doc, spec["find"], variables);
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/replaceOne/
        //   "Returns null if any argument resolves to null."
        if (find == BsonNull.Value) return BsonNull.Value;
        var replacement = Evaluate(doc, spec["replacement"], variables);
        if (replacement == BsonNull.Value) return BsonNull.Value;
        var idx = input.AsString.IndexOf(find.AsString, StringComparison.Ordinal);
        if (idx < 0) return input;
        return new BsonString(input.AsString[..idx] + replacement.AsString + input.AsString[(idx + find.AsString.Length)..]);
    }

    private static BsonValue EvalReplaceAll(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var spec = args.AsBsonDocument;
        var input = Evaluate(doc, spec["input"], variables);
        if (input == BsonNull.Value) return BsonNull.Value;
        var find = Evaluate(doc, spec["find"], variables);
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/replaceAll/
        //   "Returns null if any argument resolves to null."
        if (find == BsonNull.Value) return BsonNull.Value;
        var replacement = Evaluate(doc, spec["replacement"], variables);
        if (replacement == BsonNull.Value) return BsonNull.Value;
        return new BsonString(input.AsString.Replace(find.AsString, replacement.AsString));
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/regexMatch/
    //   "regex: The regular expression. Can be specified as a string pattern or as a regex object."
    private static (string pattern, string options) ExtractRegex(BsonDocument spec, BsonDocument? variables)
    {
        var regexVal = spec["regex"];
        if (regexVal is BsonRegularExpression bre)
            return (bre.Pattern, spec.Contains("options") ? spec["options"].AsString : bre.Options);
        return (regexVal.AsString, spec.GetValue("options", "").AsString);
    }

    private static BsonValue EvalRegexMatch(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var spec = args.AsBsonDocument;
        var input = Evaluate(doc, spec["input"], variables);
        if (input == BsonNull.Value) return BsonNull.Value;
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/regexMatch/
        //   "$regexMatch needs 'input' to be of type string"
        if (!input.IsString)
            throw MongoErrors.BadValue($"$regexMatch needs 'input' to be of type string, found: {input.BsonType}");
        var (regex, opts) = ExtractRegex(spec, variables);
        var ro = ParseRegexOptions(opts);
        return (BsonBoolean)Regex.IsMatch(input.AsString, regex, ro);
    }

    private static BsonValue EvalRegexFind(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var spec = args.AsBsonDocument;
        var input = Evaluate(doc, spec["input"], variables);
        if (input == BsonNull.Value) return BsonNull.Value;
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/regexFind/
        //   "$regexFind needs 'input' to be of type string"
        if (!input.IsString)
            throw MongoErrors.BadValue($"$regexFind needs 'input' to be of type string, found: {input.BsonType}");
        var (regex, opts) = ExtractRegex(spec, variables);
        var m = Regex.Match(input.AsString, regex, ParseRegexOptions(opts));
        if (!m.Success) return BsonNull.Value;
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/regexFind/
        //   "captures: An array that contains the matching string for each identified capture group."
        var captures = new BsonArray();
        for (int i = 1; i < m.Groups.Count; i++)
            captures.Add(m.Groups[i].Success ? (BsonValue)new BsonString(m.Groups[i].Value) : BsonNull.Value);
        return new BsonDocument { { "match", m.Value }, { "idx", m.Index }, { "captures", captures } };
    }

    private static BsonValue EvalRegexFindAll(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var spec = args.AsBsonDocument;
        var input = Evaluate(doc, spec["input"], variables);
        if (input == BsonNull.Value) return new BsonArray();
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/regexFindAll/
        //   "$regexFindAll needs 'input' to be of type string"
        if (!input.IsString)
            throw MongoErrors.BadValue($"$regexFindAll needs 'input' to be of type string, found: {input.BsonType}");
        var (regex, opts) = ExtractRegex(spec, variables);
        var matches = Regex.Matches(input.AsString, regex, ParseRegexOptions(opts));
        var result = new BsonArray();
        foreach (Match m in matches)
        {
            var captures = new BsonArray();
            for (int i = 1; i < m.Groups.Count; i++)
                captures.Add(m.Groups[i].Success ? (BsonValue)new BsonString(m.Groups[i].Value) : BsonNull.Value);
            result.Add(new BsonDocument { { "match", m.Value }, { "idx", m.Index }, { "captures", captures } });
        }
        return result;
    }

    private static RegexOptions ParseRegexOptions(string opts)
    {
        var ro = RegexOptions.None;
        if (opts.Contains('i')) ro |= RegexOptions.IgnoreCase;
        if (opts.Contains('m')) ro |= RegexOptions.Multiline;
        if (opts.Contains('s')) ro |= RegexOptions.Singleline;
        if (opts.Contains('x')) ro |= RegexOptions.IgnorePatternWhitespace;
        return ro;
    }

    #endregion

    #region Comparison

    private static BsonValue EvalCmp(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arr = EvalArray(doc, args, variables);
        return new BsonInt32(BsonValueComparer.Instance.Compare(arr[0], arr[1]));
    }

    private static BsonValue EvalCompare(BsonDocument doc, BsonValue args, BsonDocument? variables, Func<int, bool> pred)
    {
        var arr = EvalArray(doc, args, variables);
        var cmp = BsonValueComparer.Instance.Compare(arr[0], arr[1]);
        return (BsonBoolean)pred(cmp);
    }

    #endregion

    #region Conditional

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/cond/
    //   "$cond requires exactly 3 arguments in array form: [if, then, else]."
    private static BsonValue EvalCond(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        if (args is BsonArray arr3)
        {
            if (arr3.Count != 3)
                throw MongoErrors.BadValue("$cond requires exactly 3 arguments in array form: [if, then, else]");
            var condition = Evaluate(doc, arr3[0], variables);
            return IsTruthy(condition) ? Evaluate(doc, arr3[1], variables) : Evaluate(doc, arr3[2], variables);
        }
        var spec = args.AsBsonDocument;
        var cond = Evaluate(doc, spec["if"], variables);
        return IsTruthy(cond) ? Evaluate(doc, spec["then"], variables) : Evaluate(doc, spec["else"], variables);
    }

    private static BsonValue EvalIfNull(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arr = EvalArray(doc, args, variables);
        foreach (var v in arr)
        {
            if (v != BsonNull.Value) return v;
        }
        return arr[^1];
    }

    private static BsonValue EvalSwitch(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var spec = args.AsBsonDocument;
        var branches = spec["branches"].AsBsonArray;
        foreach (var branch in branches)
        {
            var b = branch.AsBsonDocument;
            var caseVal = Evaluate(doc, b["case"], variables);
            if (IsTruthy(caseVal))
                return Evaluate(doc, b["then"], variables);
        }
        if (spec.Contains("default"))
            return Evaluate(doc, spec["default"], variables);
        throw MongoErrors.BadValue("$switch: no matching branch and no default");
    }

    #endregion

    #region Boolean

    private static BsonValue EvalAnd(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arr = EvalArray(doc, args, variables);
        return (BsonBoolean)arr.All(IsTruthy);
    }

    private static BsonValue EvalOr(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arr = EvalArray(doc, args, variables);
        return (BsonBoolean)arr.Any(IsTruthy);
    }

    private static BsonValue EvalNot(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arr = EvalArray(doc, args, variables);
        return (BsonBoolean)!IsTruthy(arr[0]);
    }

    #endregion

    #region Array

    private static BsonValue EvalArrayElemAt(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arr = EvalArray(doc, args, variables);
        if (arr[0] == BsonNull.Value) return BsonNull.Value;
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/arrayElemAt/
        //   "$arrayElemAt's first argument must be an array"
        if (!arr[0].IsBsonArray)
            throw MongoErrors.BadValue($"$arrayElemAt's first argument must be an array, but is {arr[0].BsonType}");
        var array = arr[0].AsBsonArray;
        var idx = arr[1].ToInt32();
        if (idx < 0) idx += array.Count;
        if (idx < 0 || idx >= array.Count) return RemoveSentinel;
        return array[idx];
    }

    private static BsonValue EvalSize(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var val = Evaluate(doc, args is BsonArray a ? a[0] : args, variables);
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/size/
        //   "The argument for $size must resolve to an array."
        if (val == BsonNull.Value)
            throw MongoErrors.BadValue("The argument to $size must resolve to an array but was of type: null");
        if (!val.IsBsonArray)
            throw MongoErrors.BadValue($"The argument to $size must resolve to an array but was of type: {val.BsonType}");
        return new BsonInt32(val.AsBsonArray.Count);
    }

    private static BsonValue EvalIsArray(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var val = Evaluate(doc, args is BsonArray a ? a[0] : args, variables);
        return (BsonBoolean)val.IsBsonArray;
    }

    private static BsonValue EvalConcatArrays(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arr = EvalArray(doc, args, variables);
        var result = new BsonArray();
        foreach (var v in arr)
        {
            if (v == BsonNull.Value) return BsonNull.Value;
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/concatArrays/
            //   "$concatArrays only supports arrays, not <type>"
            if (!v.IsBsonArray)
                throw MongoErrors.BadValue($"$concatArrays only supports arrays, not {v.BsonType}");
            result.AddRange(v.AsBsonArray);
        }
        return result;
    }

    private static BsonValue EvalIn(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arr = EvalArray(doc, args, variables);
        var value = arr[0];
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/in/
        //   "$in requires an array as a second argument"
        if (!arr[1].IsBsonArray)
            throw MongoErrors.BadValue($"$in requires an array as a second argument, found: {arr[1].BsonType}");
        var array = arr[1].AsBsonArray;
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/in/
        //   MongoDB uses value-based comparison (cross-type numeric equality).
        return (BsonBoolean)array.Any(x => BsonValueComparer.Instance.Equals(x, value));
    }

    private static BsonValue EvalFilter(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var spec = args.AsBsonDocument;
        var input = Evaluate(doc, spec["input"], variables);
        if (input == BsonNull.Value) return BsonNull.Value;
        var asVar = spec.GetValue("as", "this").AsString;
        var cond = spec["cond"];
        var limit = spec.Contains("limit") ? Evaluate(doc, spec["limit"], variables).ToInt32() : int.MaxValue;
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/filter/
        //   "$filter's input must be an array"
        if (!input.IsBsonArray)
            throw MongoErrors.BadValue($"$filter's input must be an array, not {input.BsonType}");

        var result = new BsonArray();
        foreach (var item in input.AsBsonArray)
        {
            if (result.Count >= limit) break;
            var vars = new BsonDocument(variables ?? new BsonDocument()) { { asVar, item } };
            var condResult = Evaluate(doc, cond, vars);
            if (IsTruthy(condResult))
                result.Add(item);
        }
        return result;
    }

    private static BsonValue EvalMap(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var spec = args.AsBsonDocument;
        var input = Evaluate(doc, spec["input"], variables);
        if (input == BsonNull.Value) return BsonNull.Value;
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/map/
        //   "$map's input must be an array"
        if (!input.IsBsonArray)
            throw MongoErrors.BadValue($"$map's input must be an array, not {input.BsonType}");
        var asVar = spec.GetValue("as", "this").AsString;
        var inExpr = spec["in"];

        var result = new BsonArray();
        foreach (var item in input.AsBsonArray)
        {
            var vars = new BsonDocument(variables ?? new BsonDocument()) { { asVar, item } };
            result.Add(Evaluate(doc, inExpr, vars));
        }
        return result;
    }

    private static BsonValue EvalReduce(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var spec = args.AsBsonDocument;
        var input = Evaluate(doc, spec["input"], variables);
        if (input == BsonNull.Value) return BsonNull.Value;
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/reduce/
        //   "$reduce's input must be an array"
        if (!input.IsBsonArray)
            throw MongoErrors.BadValue($"$reduce's input must be an array, not {input.BsonType}");
        var initialValue = Evaluate(doc, spec["initialValue"], variables);
        var inExpr = spec["in"];

        var value = initialValue;
        foreach (var item in input.AsBsonArray)
        {
            var vars = new BsonDocument(variables ?? new BsonDocument()) { { "value", value }, { "this", item } };
            value = Evaluate(doc, inExpr, vars);
        }
        return value;
    }

    private static BsonValue EvalReverseArray(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var val = Evaluate(doc, args is BsonArray a ? a[0] : args, variables);
        if (val == BsonNull.Value) return BsonNull.Value;
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/reverseArray/
        //   "$reverseArray's argument must resolve to an array"
        if (!val.IsBsonArray)
            throw MongoErrors.BadValue($"$reverseArray's argument must resolve to an array, not {val.BsonType}");
        return new BsonArray(val.AsBsonArray.Reverse());
    }

    private static BsonValue EvalRange(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arr = EvalArray(doc, args, variables);
        int start = arr[0].ToInt32();
        int end = arr[1].ToInt32();
        int step = arr.Count > 2 ? arr[2].ToInt32() : 1;
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/range/
        //   "A non-zero step value."
        if (step == 0)
            throw MongoErrors.BadValue("$range requires a non-zero step value");
        var result = new BsonArray();
        for (int i = start; step > 0 ? i < end : i > end; i += step)
            result.Add(new BsonInt32(i));
        return result;
    }

    private static BsonValue EvalSlice(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arr = EvalArray(doc, args, variables);
        if (arr[0] == BsonNull.Value) return BsonNull.Value;
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/slice/
        //   "The first argument must be an array"
        if (!arr[0].IsBsonArray)
            throw MongoErrors.BadValue($"$slice's first argument must be an array, not {arr[0].BsonType}");
        var array = arr[0].AsBsonArray;
        if (arr.Count == 2)
        {
            int n = arr[1].ToInt32();
            if (n >= 0) return new BsonArray(array.Take(n));
            return new BsonArray(array.Skip(Math.Max(0, array.Count + n)));
        }
        int pos = arr[1].ToInt32();
        int count = arr[2].ToInt32();
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/slice/
        //   "If <position> is specified, <n> must resolve to a positive integer."
        if (count < 0)
            throw MongoErrors.BadValue("$slice: if position is specified, n must be a positive integer");
        if (pos < 0) pos = Math.Max(0, array.Count + pos);
        return new BsonArray(array.Skip(pos).Take(count));
    }

    private static BsonValue EvalFirst(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var val = Evaluate(doc, args is BsonArray a ? a[0] : args, variables);
        if (val == BsonNull.Value || val is not BsonArray arr || arr.Count == 0) return BsonNull.Value;
        return arr[0];
    }

    private static BsonValue EvalLast(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var val = Evaluate(doc, args is BsonArray a ? a[0] : args, variables);
        if (val == BsonNull.Value || val is not BsonArray arr || arr.Count == 0) return BsonNull.Value;
        return arr[^1];
    }

    private static BsonValue EvalSortArray(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var spec = args.AsBsonDocument;
        var input = Evaluate(doc, spec["input"], variables);
        if (input == BsonNull.Value) return BsonNull.Value;
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/sortArray/
        //   "input must resolve to an array"
        if (!input.IsBsonArray)
            throw MongoErrors.BadValue($"$sortArray's input must be an array, not {input.BsonType}");
        var sortBy = spec["sortBy"];
        if (sortBy.IsBsonDocument)
        {
            var sorted = BsonSortEvaluator.Apply(input.AsBsonArray.Select(x => x.AsBsonDocument).ToList(), sortBy.AsBsonDocument);
            return new BsonArray(sorted);
        }
        // Simple numeric sort direction
        var dir = sortBy.ToInt32();
        var items = input.AsBsonArray.ToList();
        items.Sort((a, b) => dir * BsonValueComparer.Instance.Compare(a, b));
        return new BsonArray(items);
    }

    private static BsonValue EvalObjectToArray(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var val = Evaluate(doc, args is BsonArray a ? a[0] : args, variables);
        if (val == BsonNull.Value) return BsonNull.Value;
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/objectToArray/
        //   "$objectToArray requires a document input"
        if (!val.IsBsonDocument)
            throw MongoErrors.BadValue($"$objectToArray requires a document input, found: {val.BsonType}");
        var result = new BsonArray();
        foreach (var el in val.AsBsonDocument)
            result.Add(new BsonDocument { { "k", el.Name }, { "v", el.Value } });
        return result;
    }

    private static BsonValue EvalArrayToObject(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var val = Evaluate(doc, args is BsonArray a ? a[0] : args, variables);
        if (val == BsonNull.Value) return BsonNull.Value;
        var result = new BsonDocument();
        foreach (var item in val.AsBsonArray)
        {
            if (item.IsBsonDocument)
            {
                var d = item.AsBsonDocument;
                result[d["k"].AsString] = d["v"];
            }
            else if (item.IsBsonArray)
            {
                var pair = item.AsBsonArray;
                result[pair[0].AsString] = pair[1];
            }
        }
        return result;
    }

    private static BsonValue EvalZip(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var spec = args.AsBsonDocument;
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/zip/
        //   "If any of the inputs arrays resolves to a value of null or refers to a
        //    missing field, $zip returns null."
        var evaluatedInputs = spec["inputs"].AsBsonArray.Select(i => Evaluate(doc, i, variables)).ToList();
        if (evaluatedInputs.Any(v => v == BsonNull.Value)) return BsonNull.Value;
        var inputs = evaluatedInputs.Select(v =>
        {
            if (!v.IsBsonArray)
                throw MongoErrors.BadValue($"$zip requires array inputs, found: {v.BsonType}");
            return v.AsBsonArray;
        }).ToList();
        var useLongestLength = spec.GetValue("useLongestLength", false).AsBoolean;
        var defaults = spec.Contains("defaults") ? spec["defaults"].AsBsonArray : null;

        int len = useLongestLength ? inputs.Max(a => a.Count) : inputs.Min(a => a.Count);
        var result = new BsonArray();
        for (int i = 0; i < len; i++)
        {
            var row = new BsonArray();
            for (int j = 0; j < inputs.Count; j++)
            {
                if (i < inputs[j].Count)
                    row.Add(inputs[j][i]);
                else
                    row.Add(defaults != null && j < defaults.Count ? defaults[j] : BsonNull.Value);
            }
            result.Add(row);
        }
        return result;
    }

    /// <summary>
    /// Implements $indexOfArray: searches an array for the first occurrence of a value.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/indexOfArray/
    ///   "Searches an array for an occurrence of a specified value and returns
    ///    the array index of the first occurrence."
    ///   Returns -1 if not found, null if array is null.
    /// </remarks>
    private static BsonValue EvalIndexOfArray(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arr = args.AsBsonArray;
        var arrayVal = Evaluate(doc, arr[0], variables);
        if (arrayVal == BsonNull.Value) return BsonNull.Value;
        if (!arrayVal.IsBsonArray)
            throw MongoErrors.BadValue("$indexOfArray requires an array as the first argument");
        var array = arrayVal.AsBsonArray;
        var searchVal = Evaluate(doc, arr[1], variables);
        int start = arr.Count > 2 ? (int)Evaluate(doc, arr[2], variables).ToInt64() : 0;
        int end = arr.Count > 3 ? (int)Evaluate(doc, arr[3], variables).ToInt64() : array.Count;

        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/indexOfArray/
        //   "If <start> or <end> is a negative integer, $indexOfArray returns an error."
        if (start < 0)
            throw MongoErrors.BadValue("$indexOfArray: starting index must be non-negative");
        if (end < 0)
            throw MongoErrors.BadValue("$indexOfArray: ending index must be non-negative");

        if (start > array.Count) return new BsonInt32(-1);
        if (end > array.Count) end = array.Count;

        for (int i = start; i < end; i++)
        {
            if (BsonValueComparer.Instance.Compare(array[i], searchVal) == 0)
                return new BsonInt32(i);
        }
        return new BsonInt32(-1);
    }

    #region Set Expression Operators

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/setUnion/
    //   "Returns a set with elements that appear in any of the input sets."
    private static BsonValue EvalSetUnion(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arrays = args.AsBsonArray.Select(a => Evaluate(doc, a, variables)).ToList();
        if (arrays.Any(a => a == BsonNull.Value)) return BsonNull.Value;
        var resultSet = new List<BsonValue>();
        var seen = new HashSet<BsonValue>(BsonValueComparer.Instance);
        foreach (var arr in arrays)
        {
            if (!arr.IsBsonArray)
                throw MongoErrors.BadValue("All operands of $setUnion must be arrays.");
            foreach (var elem in arr.AsBsonArray)
            {
                if (seen.Add(elem))
                    resultSet.Add(elem);
            }
        }
        return new BsonArray(resultSet);
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/setIntersection/
    //   "Returns a set with elements that appear in all of the input sets."
    private static BsonValue EvalSetIntersection(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arrays = args.AsBsonArray.Select(a => Evaluate(doc, a, variables)).ToList();
        if (arrays.Any(a => a == BsonNull.Value)) return BsonNull.Value;
        var sets = arrays.Select(a =>
        {
            if (!a.IsBsonArray)
                throw MongoErrors.BadValue("All operands of $setIntersection must be arrays.");
            return new HashSet<BsonValue>(a.AsBsonArray, BsonValueComparer.Instance);
        }).ToList();
        if (sets.Count == 0) return new BsonArray();
        var intersection = sets[0];
        for (int i = 1; i < sets.Count; i++)
            intersection.IntersectWith(sets[i]);
        return new BsonArray(intersection);
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/setDifference/
    //   "Returns a set with elements that appear in the first set but not in the second set."
    private static BsonValue EvalSetDifference(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arr = args.AsBsonArray;
        var first = Evaluate(doc, arr[0], variables);
        var second = Evaluate(doc, arr[1], variables);
        if (first == BsonNull.Value || second == BsonNull.Value) return BsonNull.Value;
        if (!first.IsBsonArray || !second.IsBsonArray)
            throw MongoErrors.BadValue("Both operands of $setDifference must be arrays.");
        var secondSet = new HashSet<BsonValue>(second.AsBsonArray, BsonValueComparer.Instance);
        var result = new BsonArray();
        var seen = new HashSet<BsonValue>(BsonValueComparer.Instance);
        foreach (var elem in first.AsBsonArray)
        {
            if (!secondSet.Contains(elem) && seen.Add(elem))
                result.Add(elem);
        }
        return result;
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/setEquals/
    //   "Returns true if the input sets have the same distinct elements."
    private static BsonValue EvalSetEquals(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arrays = args.AsBsonArray.Select(a => Evaluate(doc, a, variables)).ToList();
        var sets = arrays.Select(a =>
        {
            if (!a.IsBsonArray)
                throw MongoErrors.BadValue("All operands of $setEquals must be arrays.");
            return new HashSet<BsonValue>(a.AsBsonArray, BsonValueComparer.Instance);
        }).ToList();
        if (sets.Count < 2) throw MongoErrors.BadValue("$setEquals requires at least two arguments.");
        var first = sets[0];
        return new BsonBoolean(sets.Skip(1).All(s => s.SetEquals(first)));
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/setIsSubset/
    //   "Returns true if all elements of the first set appear in the second set."
    private static BsonValue EvalSetIsSubset(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arr = args.AsBsonArray;
        var first = Evaluate(doc, arr[0], variables);
        var second = Evaluate(doc, arr[1], variables);
        if (!first.IsBsonArray || !second.IsBsonArray)
            throw MongoErrors.BadValue("Both operands of $setIsSubset must be arrays.");
        var secondSet = new HashSet<BsonValue>(second.AsBsonArray, BsonValueComparer.Instance);
        return new BsonBoolean(first.AsBsonArray.All(e => secondSet.Contains(e)));
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/anyElementTrue/
    //   "Returns true if any elements of a set evaluate to true."
    private static BsonValue EvalAnyElementTrue(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var input = args is BsonArray arr ? Evaluate(doc, arr[0], variables) : Evaluate(doc, args, variables);
        if (!input.IsBsonArray)
            throw MongoErrors.BadValue("$anyElementTrue's argument must be an array.");
        return new BsonBoolean(input.AsBsonArray.Any(e => IsTruthy(e)));
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/allElementsTrue/
    //   "Returns true if no element of a set evaluates to false."
    private static BsonValue EvalAllElementsTrue(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var input = args is BsonArray arr ? Evaluate(doc, arr[0], variables) : Evaluate(doc, args, variables);
        if (!input.IsBsonArray)
            throw MongoErrors.BadValue("$allElementsTrue's argument must be an array.");
        return new BsonBoolean(input.AsBsonArray.All(e => IsTruthy(e)));
    }

    #endregion

    #endregion

    #region Type

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/type/
    private static BsonValue EvalType(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var val = Evaluate(doc, args is BsonArray a ? a[0] : args, variables);
        return new BsonString(val.BsonType switch
        {
            BsonType.Double => "double",
            BsonType.String => "string",
            BsonType.Document => "object",
            BsonType.Array => "array",
            BsonType.Binary => "binData",
            BsonType.ObjectId => "objectId",
            BsonType.Boolean => "bool",
            BsonType.DateTime => "date",
            BsonType.Null => "null",
            BsonType.RegularExpression => "regex",
            BsonType.Int32 => "int",
            BsonType.Timestamp => "timestamp",
            BsonType.Int64 => "long",
            BsonType.Decimal128 => "decimal",
            _ => "missing"
        });
    }

    private static BsonValue EvalConvert(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var spec = args.AsBsonDocument;
        var input = Evaluate(doc, spec["input"], variables);
        var toVal = Evaluate(doc, spec["to"], variables);
        var onError = spec.Contains("onError") ? spec["onError"] : null;
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/convert/
        //   "onNull: The value to return if the input is null or missing."
        var onNull = spec.Contains("onNull") ? spec["onNull"] : null;

        if (input == BsonNull.Value)
            return onNull != null ? Evaluate(doc, onNull, variables) : BsonNull.Value;

        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/convert/
        //   "to: Can be any valid expression that resolves to one of the following numeric
        //    or string identifiers."
        var to = toVal.IsNumeric ? BsonTypeCodeToName(toVal.ToInt32()) : toVal.AsString;
        try
        {
            return ConvertTo(input, to);
        }
        catch
        {
            if (onError != null) return Evaluate(doc, onError, variables);
            throw;
        }
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/convert/
    //   "to: ... numeric BSON type identifiers."
    private static string BsonTypeCodeToName(int code) => code switch
    {
        1 => "double",
        2 => "string",
        7 => "objectId",
        8 => "bool",
        9 => "date",
        16 => "int",
        18 => "long",
        19 => "decimal",
        _ => throw MongoErrors.BadValue($"Unknown BSON type code: {code}")
    };

    private static BsonValue ConvertTo(BsonValue input, string type)
    {
        if (input == BsonNull.Value) return BsonNull.Value;
        return type switch
        {
            "double" => new BsonDouble(input.ToDouble()),
            "string" => new BsonString(input.ToString()!),
            "bool" => (BsonBoolean)IsTruthy(input),
            "int" => new BsonInt32(input.ToInt32()),
            "long" => new BsonInt64(input.ToInt64()),
            "decimal" => new BsonDecimal128(input.ToDecimal()),
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/convert/
            //   "Returns a date that corresponds to the timestamp of the ObjectId."
            "date" => input.IsObjectId
                ? new BsonDateTime(input.AsObjectId.CreationTime)
                : input.IsString
                    ? new BsonDateTime(DateTime.Parse(input.AsString, CultureInfo.InvariantCulture))
                    : new BsonDateTime(BsonUtils.ToDateTimeFromMillisecondsSinceEpoch(input.ToInt64())),
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/convert/
            //   Only string inputs are valid for conversion to ObjectId.
            "objectId" => input.IsString
                ? new BsonObjectId(ObjectId.Parse(input.AsString))
                : throw MongoErrors.BadValue($"Failed to parse objectId from: {input}"),
            _ => throw MongoErrors.BadValue($"$convert: unknown type '{type}'")
        };
    }

    private static BsonValue EvalToBool(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var val = Evaluate(doc, args is BsonArray a ? a[0] : args, variables);
        return (BsonBoolean)IsTruthy(val);
    }

    private static BsonValue EvalToInt(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var val = Evaluate(doc, args is BsonArray a ? a[0] : args, variables);
        if (val == BsonNull.Value) return BsonNull.Value;
        return new BsonInt32(val.ToInt32());
    }

    private static BsonValue EvalToLong(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var val = Evaluate(doc, args is BsonArray a ? a[0] : args, variables);
        if (val == BsonNull.Value) return BsonNull.Value;
        return new BsonInt64(val.ToInt64());
    }

    private static BsonValue EvalToDouble(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var val = Evaluate(doc, args is BsonArray a ? a[0] : args, variables);
        if (val == BsonNull.Value) return BsonNull.Value;
        return new BsonDouble(val.ToDouble());
    }

    private static BsonValue EvalToDecimal(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var val = Evaluate(doc, args is BsonArray a ? a[0] : args, variables);
        if (val == BsonNull.Value) return BsonNull.Value;
        return new BsonDecimal128(val.ToDecimal());
    }

    private static BsonValue EvalToString(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var val = Evaluate(doc, args is BsonArray a ? a[0] : args, variables);
        if (val == BsonNull.Value) return BsonNull.Value;
        return new BsonString(val.ToString()!);
    }

    private static BsonValue EvalToDate(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var val = Evaluate(doc, args is BsonArray a ? a[0] : args, variables);
        if (val == BsonNull.Value) return BsonNull.Value;
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/toDate/
        //   "$toDate is equivalent to { $convert: { input: <expression>, to: 'date' } }"
        if (val.IsObjectId) return new BsonDateTime(val.AsObjectId.CreationTime);
        if (val.IsString) return new BsonDateTime(DateTime.Parse(val.AsString, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal));
        return new BsonDateTime(BsonUtils.ToDateTimeFromMillisecondsSinceEpoch(val.ToInt64()));
    }

    private static BsonValue EvalToObjectId(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var val = Evaluate(doc, args is BsonArray a ? a[0] : args, variables);
        if (val == BsonNull.Value) return BsonNull.Value;
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/toObjectId/
        //   "$toObjectId requires a string argument"
        if (!val.IsString)
            throw MongoErrors.BadValue($"$toObjectId requires a string argument, found: {val.BsonType}");
        try
        {
            return new BsonObjectId(ObjectId.Parse(val.AsString));
        }
        catch (FormatException)
        {
            throw MongoErrors.BadValue($"$toObjectId: '{val.AsString}' is not a valid ObjectId");
        }
    }

    private static BsonValue EvalIsNumber(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var val = Evaluate(doc, args is BsonArray a ? a[0] : args, variables);
        return (BsonBoolean)(val.BsonType is BsonType.Int32 or BsonType.Int64 or BsonType.Double or BsonType.Decimal128);
    }

    #endregion

    #region Date

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation-date/
    private static BsonValue EvalDatePart(BsonDocument doc, BsonValue args, BsonDocument? variables, Func<DateTime, int> fn)
    {
        var val = Evaluate(doc, args is BsonArray a ? a[0] : args, variables);
        if (val == BsonNull.Value) return BsonNull.Value;
        var dt = val.ToUniversalTime();
        return new BsonInt32(fn(dt));
    }

    private static BsonValue EvalDateToString(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var spec = args.AsBsonDocument;
        var date = Evaluate(doc, spec["date"], variables);
        if (date == BsonNull.Value) return spec.Contains("onNull") ? Evaluate(doc, spec["onNull"], variables) : BsonNull.Value;
        var format = spec.GetValue("format", "%Y-%m-%dT%H:%M:%S.%LZ").AsString;
        var dt = date.ToUniversalTime();
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateToString/
        //   "%Z: The minute offset from UTC as a number. For example, if the offset is +530, the return string will be +0530."
        //   MongoDB outputs timezone offset without colon separator (e.g., +0000 not +00:00).
        var hasPercentZ = format.Contains("%Z");
        // Convert MongoDB format to .NET format
        var netFormat = format
            .Replace("%Y", "yyyy").Replace("%m", "MM").Replace("%d", "dd")
            .Replace("%H", "HH").Replace("%M", "mm").Replace("%S", "ss")
            .Replace("%L", "fff").Replace("%Z", "zzz");
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateToString/
        //   "%j: Day of year (3 digits, zero padded). Valid values: 001-366."
        //   .NET has no built-in single format specifier for day-of-year, so we handle it manually.
        if (netFormat.Contains("%j"))
            netFormat = netFormat.Replace("%j", dt.DayOfYear.ToString("D3"));
        var result = dt.ToString(netFormat, CultureInfo.InvariantCulture);
        // Remove colon from timezone offset to match MongoDB format (+00:00 → +0000)
        if (hasPercentZ)
        {
            var idx = result.LastIndexOf(':');
            if (idx >= 3 && (result[idx - 3] == '+' || result[idx - 3] == '-'))
                result = result.Remove(idx, 1);
        }
        return new BsonString(result);
    }

    private static BsonValue EvalDateFromString(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var spec = args.AsBsonDocument;
        var dateString = Evaluate(doc, spec["dateString"], variables);
        if (dateString == BsonNull.Value) return spec.Contains("onNull") ? Evaluate(doc, spec["onNull"], variables) : BsonNull.Value;
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateFromString/
        //   "dateString must be a string"
        if (!dateString.IsString)
            throw MongoErrors.BadValue($"$dateFromString requires a string as 'dateString', found: {dateString.BsonType}");
        try
        {
            DateTime dt;
            if (spec.Contains("format"))
            {
                // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateFromString/
                //   "format: Optional. The date format specification of the dateString.
                //    format can be any expression that evaluates to a string literal,
                //    containing 0 or more format specifiers."
                var mongoFormat = Evaluate(doc, spec["format"], variables).AsString;
                var dotnetFormat = ConvertMongoDateFormatToDotNet(mongoFormat);
                dt = DateTime.ParseExact(dateString.AsString, dotnetFormat, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
            }
            else
            {
                dt = DateTime.Parse(dateString.AsString, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
            }

            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateFromString/
            //   "timezone: Optional. The time zone to use to format the date."
            //   If timezone is specified, the parsed date is interpreted in that timezone and converted to UTC.
            if (spec.Contains("timezone"))
            {
                var tz = Evaluate(doc, spec["timezone"], variables).AsString;
                var offset = ParseTimezoneOffset(tz);
                dt = dt.Add(-offset); // Convert from local timezone to UTC
            }

            return new BsonDateTime(dt);
        }
        catch
        {
            if (spec.Contains("onError")) return Evaluate(doc, spec["onError"], variables);
            throw;
        }
    }

    /// <summary>
    /// Converts MongoDB strftime-style format to .NET DateTime format.
    /// </summary>
    private static string ConvertMongoDateFormatToDotNet(string mongoFormat)
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateFromString/
        //   MongoDB uses strftime-style format specifiers
        return mongoFormat
            .Replace("%Y", "yyyy")
            .Replace("%m", "MM")
            .Replace("%d", "dd")
            .Replace("%H", "HH")
            .Replace("%M", "mm")
            .Replace("%S", "ss")
            .Replace("%L", "fff")
            .Replace("%j", "DDD")
            .Replace("%w", "d")
            .Replace("%U", "ww")
            .Replace("%Z", "zzz")
            .Replace("%z", "zzz")
            .Replace("%%", "%");
    }

    /// <summary>
    /// Parses a timezone string (e.g., "+05:00", "-08:00", "UTC") into a TimeSpan offset.
    /// </summary>
    private static TimeSpan ParseTimezoneOffset(string tz)
    {
        if (tz == "UTC" || tz == "GMT") return TimeSpan.Zero;
        if (tz.StartsWith("+") || tz.StartsWith("-"))
        {
            return TimeSpan.Parse(tz.TrimStart('+'));
        }
        // Try as IANA timezone name
        try
        {
            var tzi = TimeZoneInfo.FindSystemTimeZoneById(tz);
            return tzi.BaseUtcOffset;
        }
        catch
        {
            return TimeSpan.Zero;
        }
    }

    private static BsonValue EvalDateAdd(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var spec = args.AsBsonDocument;
        var startDate = Evaluate(doc, spec["startDate"], variables).ToUniversalTime();
        var unit = spec["unit"].AsString;
        var amount = Evaluate(doc, spec["amount"], variables).ToInt64();
        return new BsonDateTime(AddToDate(startDate, unit, amount));
    }

    private static BsonValue EvalDateSubtract(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var spec = args.AsBsonDocument;
        var startDate = Evaluate(doc, spec["startDate"], variables).ToUniversalTime();
        var unit = spec["unit"].AsString;
        var amount = Evaluate(doc, spec["amount"], variables).ToInt64();
        return new BsonDateTime(AddToDate(startDate, unit, -amount));
    }

    private static BsonValue EvalDateDiff(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var spec = args.AsBsonDocument;
        var start = Evaluate(doc, spec["startDate"], variables).ToUniversalTime();
        var end = Evaluate(doc, spec["endDate"], variables).ToUniversalTime();
        var unit = spec["unit"].AsString;
        var diff = end - start;
        long result = unit switch
        {
            "millisecond" => (long)diff.TotalMilliseconds,
            "second" => (long)diff.TotalSeconds,
            "minute" => (long)diff.TotalMinutes,
            "hour" => (long)diff.TotalHours,
            "day" => (long)diff.TotalDays,
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateDiff/
            //   "week" counts weekly boundaries crossed. Default startOfWeek is Sunday.
            "week" => CountWeekBoundaries(start, end, spec.Contains("startOfWeek") ? spec["startOfWeek"].AsString : "sunday"),
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateDiff/
            //   "month" counts calendar month boundaries crossed.
            "month" => (end.Year - start.Year) * 12 + (end.Month - start.Month),
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateDiff/
            //   "quarter" counts quarter boundaries crossed (Jan 1, Apr 1, Jul 1, Oct 1).
            "quarter" => (end.Year * 4 + (end.Month - 1) / 3) - (start.Year * 4 + (start.Month - 1) / 3),
            "year" => end.Year - start.Year,
            _ => throw MongoErrors.BadValue($"Unknown date unit: {unit}")
        };
        return new BsonInt64(result);
    }

    private static long CountWeekBoundaries(DateTime start, DateTime end, string startOfWeek)
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateDiff/
        //   Counts the number of times the startOfWeek day is crossed between start and end.
        var dow = startOfWeek.ToLowerInvariant() switch
        {
            "sunday" or "sun" => DayOfWeek.Sunday,
            "monday" or "mon" => DayOfWeek.Monday,
            "tuesday" or "tue" => DayOfWeek.Tuesday,
            "wednesday" or "wed" => DayOfWeek.Wednesday,
            "thursday" or "thu" => DayOfWeek.Thursday,
            "friday" or "fri" => DayOfWeek.Friday,
            "saturday" or "sat" => DayOfWeek.Saturday,
            _ => DayOfWeek.Sunday
        };

        if (end < start) return -CountWeekBoundaries(end, start, startOfWeek);
        if (end == start) return 0;

        // Days until next boundary from start
        int startDaysUntil = ((int)dow - (int)start.DayOfWeek + 7) % 7;
        if (startDaysUntil == 0) startDaysUntil = 7; // current day doesn't count as a crossed boundary
        var firstBoundary = start.AddDays(startDaysUntil);

        if (firstBoundary > end) return 0;

        return (long)((end - firstBoundary).TotalDays / 7) + 1;
    }

    private static DateTime AddToDate(DateTime dt, string unit, long amount)
    {
        return unit switch
        {
            "millisecond" => dt.AddMilliseconds(amount),
            "second" => dt.AddSeconds(amount),
            "minute" => dt.AddMinutes(amount),
            "hour" => dt.AddHours(amount),
            "day" => dt.AddDays(amount),
            "week" => dt.AddDays(amount * 7),
            "month" => dt.AddMonths((int)amount),
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateAdd/
            //   unit supports "quarter" (1 quarter = 3 months)
            "quarter" => dt.AddMonths((int)amount * 3),
            "year" => dt.AddYears((int)amount),
            _ => throw MongoErrors.BadValue($"Unknown date unit: {unit}")
        };
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateTrunc/
    //   "Truncates a date."
    private static BsonValue EvalDateTrunc(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var spec = args.AsBsonDocument;
        var dateVal = Evaluate(doc, spec["date"], variables);
        if (dateVal == BsonNull.Value) return BsonNull.Value;
        var dt = dateVal.ToUniversalTime();
        var unit = spec["unit"].AsString;
        var binSize = spec.Contains("binSize") ? (int)Evaluate(doc, spec["binSize"], variables).ToInt64() : 1;

        // Reference point for binning: 2000-01-01T00:00:00Z
        var refDate = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        DateTime truncated;
        switch (unit)
        {
            case "year":
                if (binSize == 1) { truncated = new DateTime(dt.Year, 1, 1, 0, 0, 0, DateTimeKind.Utc); }
                else
                {
                    int yearsSinceRef = dt.Year - refDate.Year;
                    int binStart = yearsSinceRef / binSize * binSize;
                    truncated = new DateTime(refDate.Year + binStart, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                }
                break;
            case "quarter":
                if (binSize == 1)
                {
                    int qMonth = ((dt.Month - 1) / 3) * 3 + 1;
                    truncated = new DateTime(dt.Year, qMonth, 1, 0, 0, 0, DateTimeKind.Utc);
                }
                else
                {
                    int totalQuarters = (dt.Year - refDate.Year) * 4 + (dt.Month - 1) / 3;
                    int binStart = totalQuarters / binSize * binSize;
                    int baseYear = refDate.Year + binStart / 4;
                    int baseMonth = (binStart % 4) * 3 + 1;
                    truncated = new DateTime(baseYear, baseMonth, 1, 0, 0, 0, DateTimeKind.Utc);
                }
                break;
            case "month":
                if (binSize == 1) { truncated = new DateTime(dt.Year, dt.Month, 1, 0, 0, 0, DateTimeKind.Utc); }
                else
                {
                    int totalMonths = (dt.Year - refDate.Year) * 12 + (dt.Month - 1);
                    int binStart = totalMonths / binSize * binSize;
                    int baseYear = refDate.Year + binStart / 12;
                    int baseMonth = binStart % 12 + 1;
                    truncated = new DateTime(baseYear, baseMonth, 1, 0, 0, 0, DateTimeKind.Utc);
                }
                break;
            case "week":
                var startOfWeekStr = spec.GetValue("startOfWeek", "sunday").AsString.ToLowerInvariant();
                var dow = startOfWeekStr switch
                {
                    "monday" or "mon" => DayOfWeek.Monday,
                    "tuesday" or "tue" => DayOfWeek.Tuesday,
                    "wednesday" or "wed" => DayOfWeek.Wednesday,
                    "thursday" or "thu" => DayOfWeek.Thursday,
                    "friday" or "fri" => DayOfWeek.Friday,
                    "saturday" or "sat" => DayOfWeek.Saturday,
                    _ => DayOfWeek.Sunday
                };
                int daysSinceStart = ((int)dt.DayOfWeek - (int)dow + 7) % 7;
                var weekStart = dt.Date.AddDays(-daysSinceStart);
                if (binSize == 1) { truncated = weekStart; }
                else
                {
                    // Find reference Sunday (or startOfWeek) >= 2000-01-01
                    var refWeekStart = refDate;
                    int refDaysSince = ((int)refWeekStart.DayOfWeek - (int)dow + 7) % 7;
                    if (refDaysSince > 0) refWeekStart = refWeekStart.AddDays(7 - refDaysSince);
                    long totalWeeks = (long)(weekStart - refWeekStart).TotalDays / 7;
                    long binStart = totalWeeks / binSize * binSize;
                    truncated = refWeekStart.AddDays(binStart * 7);
                }
                break;
            case "day":
                if (binSize == 1) { truncated = dt.Date; }
                else
                {
                    long totalDays = (long)(dt.Date - refDate).TotalDays;
                    long binStart = totalDays / binSize * binSize;
                    truncated = refDate.AddDays(binStart);
                }
                break;
            case "hour":
                if (binSize == 1) { truncated = new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, 0, 0, DateTimeKind.Utc); }
                else
                {
                    long totalHours = (long)(dt - refDate).TotalHours;
                    long binStart = totalHours / binSize * binSize;
                    truncated = refDate.AddHours(binStart);
                }
                break;
            case "minute":
                if (binSize == 1) { truncated = new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, 0, DateTimeKind.Utc); }
                else
                {
                    long totalMinutes = (long)(dt - refDate).TotalMinutes;
                    long binStart = totalMinutes / binSize * binSize;
                    truncated = refDate.AddMinutes(binStart);
                }
                break;
            case "second":
                if (binSize == 1) { truncated = new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, DateTimeKind.Utc); }
                else
                {
                    long totalSeconds = (long)(dt - refDate).TotalSeconds;
                    long binStart = totalSeconds / binSize * binSize;
                    truncated = refDate.AddSeconds(binStart);
                }
                break;
            default:
                throw MongoErrors.BadValue($"Unknown date unit: {unit}");
        }
        return new BsonDateTime(truncated);
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateFromParts/
    //   "Constructs and returns a Date object given the date's constituent properties."
    private static BsonValue EvalDateFromParts(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var spec = args.AsBsonDocument;

        if (spec.Contains("isoWeekYear"))
        {
            // ISO week date form
            var isoWeekYear = (int)Evaluate(doc, spec["isoWeekYear"], variables).ToInt64();
            var isoWeek = spec.Contains("isoWeek") ? (int)Evaluate(doc, spec["isoWeek"], variables).ToInt64() : 1;
            var isoDayOfWeek = spec.Contains("isoDayOfWeek") ? (int)Evaluate(doc, spec["isoDayOfWeek"], variables).ToInt64() : 1;
            var hour = spec.Contains("hour") ? (int)Evaluate(doc, spec["hour"], variables).ToInt64() : 0;
            var minute = spec.Contains("minute") ? (int)Evaluate(doc, spec["minute"], variables).ToInt64() : 0;
            var second = spec.Contains("second") ? (int)Evaluate(doc, spec["second"], variables).ToInt64() : 0;
            var ms = spec.Contains("millisecond") ? (int)Evaluate(doc, spec["millisecond"], variables).ToInt64() : 0;

            // Convert ISO week date to Gregorian
            var dt = System.Globalization.ISOWeek.ToDateTime(isoWeekYear, isoWeek, (DayOfWeek)(isoDayOfWeek % 7));
            dt = dt.AddHours(hour).AddMinutes(minute).AddSeconds(second).AddMilliseconds(ms);
            return new BsonDateTime(DateTime.SpecifyKind(dt, DateTimeKind.Utc));
        }
        else
        {
            // Calendar date form
            var year = (int)Evaluate(doc, spec["year"], variables).ToInt64();
            var month = spec.Contains("month") ? (int)Evaluate(doc, spec["month"], variables).ToInt64() : 1;
            var day = spec.Contains("day") ? (int)Evaluate(doc, spec["day"], variables).ToInt64() : 1;
            var hour = spec.Contains("hour") ? (int)Evaluate(doc, spec["hour"], variables).ToInt64() : 0;
            var minute = spec.Contains("minute") ? (int)Evaluate(doc, spec["minute"], variables).ToInt64() : 0;
            var second = spec.Contains("second") ? (int)Evaluate(doc, spec["second"], variables).ToInt64() : 0;
            var ms = spec.Contains("millisecond") ? (int)Evaluate(doc, spec["millisecond"], variables).ToInt64() : 0;

            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateFromParts/
            //   "If the number specified is outside this range, $dateFromParts incorporates the difference"
            // Handle overflow: start from year-01-01 and add offsets
            var dt = new DateTime(year, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            dt = dt.AddMonths(month - 1).AddDays(day - 1)
                   .AddHours(hour).AddMinutes(minute).AddSeconds(second).AddMilliseconds(ms);
            return new BsonDateTime(dt);
        }
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/dateToParts/
    //   "Returns a document that contains the constituent parts of a given Date value."
    private static BsonValue EvalDateToParts(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var spec = args.AsBsonDocument;
        var dateVal = Evaluate(doc, spec["date"], variables);
        if (dateVal == BsonNull.Value) return BsonNull.Value;
        var dt = dateVal.ToUniversalTime();
        var iso8601 = spec.Contains("iso8601") && spec["iso8601"].AsBoolean;

        if (iso8601)
        {
            return new BsonDocument
            {
                { "isoWeekYear", System.Globalization.ISOWeek.GetYear(dt) },
                { "isoWeek", System.Globalization.ISOWeek.GetWeekOfYear(dt) },
                { "isoDayOfWeek", (int)dt.DayOfWeek == 0 ? 7 : (int)dt.DayOfWeek },
                { "hour", dt.Hour },
                { "minute", dt.Minute },
                { "second", dt.Second },
                { "millisecond", dt.Millisecond }
            };
        }
        else
        {
            return new BsonDocument
            {
                { "year", dt.Year },
                { "month", dt.Month },
                { "day", dt.Day },
                { "hour", dt.Hour },
                { "minute", dt.Minute },
                { "second", dt.Second },
                { "millisecond", dt.Millisecond }
            };
        }
    }

    #endregion

    #region Object

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/mergeObjects/
    private static BsonValue EvalMergeObjects(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var arr = EvalArray(doc, args, variables);
        var result = new BsonDocument();
        foreach (var v in arr)
        {
            if (v == BsonNull.Value) continue;
            //   "$mergeObjects requires object inputs"
            if (!v.IsBsonDocument)
                throw MongoErrors.BadValue($"$mergeObjects requires object inputs, but received: {v.BsonType}");
            foreach (var el in v.AsBsonDocument)
                result[el.Name] = el.Value;
        }
        return result;
    }

    private static BsonValue EvalGetField(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        if (args.IsString)
            return BsonFilterEvaluator.ResolveFieldPath(doc, args.AsString);
        var spec = args.AsBsonDocument;
        var field = Evaluate(doc, spec["field"], variables).AsString;
        // Ref: Observed real MongoDB 7.0:
        //   When input resolves to null/missing, $getField omits the field from output entirely.
        var inputVal = spec.Contains("input") ? Evaluate(doc, spec["input"], variables) : (BsonValue)doc;
        if (inputVal == BsonNull.Value || inputVal.IsBsonNull) return RemoveSentinel;
        var input = inputVal.AsBsonDocument;
        return input.Contains(field) ? input[field] : RemoveSentinel;
    }

    private static BsonValue EvalSetField(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var spec = args.AsBsonDocument;
        var field = Evaluate(doc, spec["field"], variables).AsString;
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/setField/
        //   "If the input argument resolves to null or missing, $setField returns null."
        var inputVal = Evaluate(doc, spec["input"], variables);
        if (inputVal == BsonNull.Value) return BsonNull.Value;
        var input = inputVal.AsBsonDocument.DeepClone().AsBsonDocument;
        var value = Evaluate(doc, spec["value"], variables);
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/setField/
        //   "If the value resolves to $$REMOVE, the field is removed from the document."
        if (IsRemove(value))
            input.Remove(field);
        else
            input[field] = value;
        return input;
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/unsetField/
    //   "Removes a field from a document. Alias for $setField with value: $$REMOVE."
    private static BsonValue EvalUnsetField(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var spec = args.AsBsonDocument;
        var field = Evaluate(doc, spec["field"], variables).AsString;
        var inputVal = Evaluate(doc, spec["input"], variables);
        if (inputVal == BsonNull.Value) return BsonNull.Value;
        var input = inputVal.AsBsonDocument.DeepClone().AsBsonDocument;
        input.Remove(field);
        return input;
    }

    #endregion

    #region Let

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/let/
    private static BsonValue EvalLet(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var spec = args.AsBsonDocument;
        var vars = spec["vars"].AsBsonDocument;
        var inExpr = spec["in"];
        var newVars = new BsonDocument(variables ?? new BsonDocument());
        foreach (var v in vars)
            newVars[v.Name] = Evaluate(doc, v.Value, variables);
        return Evaluate(doc, inExpr, newVars);
    }

    #endregion

    #region Bitwise

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/bitAnd/
    //   "Returns the result of a bitwise and operation on an array of int or long values."
    private static BsonValue EvalBitwiseAggregate(BsonDocument doc, BsonValue args, BsonDocument? variables, Func<long, long, long> op)
    {
        var arr = EvalArray(doc, args, variables);
        bool isLong = arr.Any(v => v.BsonType == BsonType.Int64);
        long result = arr[0].ToInt64();
        for (int i = 1; i < arr.Count; i++)
            result = op(result, arr[i].ToInt64());
        return isLong ? (BsonValue)new BsonInt64(result) : new BsonInt32((int)result);
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/bitNot/
    //   "Returns the result of a bitwise not operation on a single argument."
    private static BsonValue EvalBitNot(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var val = Evaluate(doc, args is BsonArray a ? a[0] : args, variables);
        if (val.BsonType == BsonType.Int64) return new BsonInt64(~val.ToInt64());
        return new BsonInt32(~val.ToInt32());
    }

    #endregion

    #region Helpers

    /// <summary>
    /// Evaluate an array of expressions, or if args is already evaluated, wrap single value.
    /// </summary>
    private static List<BsonValue> EvalArray(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        if (args is BsonArray arr)
            return arr.Select(a => Evaluate(doc, a, variables)).ToList();
        return [Evaluate(doc, args, variables)];
    }

    /// <summary>
    /// MongoDB truthiness: false, null, 0, undefined → false; everything else → true.
    /// </summary>
    internal static bool IsTruthy(BsonValue value)
    {
        if (value == BsonNull.Value) return false;
        if (value.IsBoolean) return value.AsBoolean;
        if (value.IsNumeric) return value.ToDouble() != 0;
        return true;
    }

    #endregion

    #region Size Operators

    private static BsonValue EvalBinarySize(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var val = Evaluate(doc, args, variables);
        if (val == BsonNull.Value) return BsonNull.Value;
        if (val.BsonType == BsonType.String)
            return new BsonInt32(System.Text.Encoding.UTF8.GetByteCount(val.AsString));
        if (val.BsonType == BsonType.Binary)
            return new BsonInt32(val.AsByteArray.Length);
        return BsonNull.Value;
    }

    private static BsonValue EvalBsonSize(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var val = Evaluate(doc, args, variables);
        if (val == BsonNull.Value) return BsonNull.Value;
        if (val is BsonDocument bsonDoc)
            return new BsonInt32(bsonDoc.ToBson().Length);
        return BsonNull.Value;
    }

    #endregion

    #region Hash / Timestamp Operators

    private static BsonValue EvalToHashedIndexKey(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var val = Evaluate(doc, args, variables);
        // Use a simple hash — MongoDB uses md5-based hashing internally
        var hash = val.GetHashCode();
        return new BsonInt64(hash);
    }

    private static BsonValue EvalTsIncrement(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var val = Evaluate(doc, args, variables);
        if (val.BsonType != BsonType.Timestamp) return BsonNull.Value;
        return new BsonInt64(val.AsBsonTimestamp.Increment);
    }

    private static BsonValue EvalTsSecond(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var val = Evaluate(doc, args, variables);
        if (val.BsonType != BsonType.Timestamp) return BsonNull.Value;
        return new BsonInt64(val.AsBsonTimestamp.Timestamp);
    }

    #endregion

    #region $function

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/function/
    //   "Defines a custom aggregation function or expression in JavaScript."
    //   "The body of the function is a string containing a JavaScript function."
    // Note: Actual JavaScript execution requires the JsTriggers package.
    private static Func<string, BsonArray, BsonValue>? _functionEvaluator;

    /// <summary>
    /// Registers a custom $function evaluator (typically from the JsTriggers package).
    /// </summary>
    internal static void RegisterFunctionEvaluator(Func<string, BsonArray, BsonValue> evaluator)
    {
        _functionEvaluator = evaluator;
    }

    private static BsonValue EvalFunction(BsonDocument doc, BsonValue args, BsonDocument? variables)
    {
        var spec = args.AsBsonDocument;
        var body = spec["body"].AsString;
        var fnArgs = spec.Contains("args") ? spec["args"].AsBsonArray : new BsonArray();

        // Evaluate argument expressions against the current document
        var evaluatedArgs = new BsonArray();
        foreach (var arg in fnArgs)
        {
            evaluatedArgs.Add(Evaluate(doc, arg, variables));
        }

        if (_functionEvaluator != null)
            return _functionEvaluator(body, evaluatedArgs);

        throw new NotSupportedException(
            "$function requires the InMemoryEmulator.MongoDB.JsTriggers package for JavaScript execution. " +
            "Install the package and call JsExpressionSetup.Register() to enable $function support.");
    }

    #endregion

    #region $accumulator (hook)

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/accumulator/
    //   "Defines a custom accumulator function in JavaScript."
    private static Func<BsonDocument, List<BsonDocument>, BsonDocument?, BsonValue>? _accumulatorEvaluator;

    /// <summary>
    /// Registers a custom $accumulator evaluator (typically from the JsTriggers package).
    /// Takes (accSpec, groupDocs, variables) and returns the accumulated result.
    /// </summary>
    internal static void RegisterAccumulatorEvaluator(Func<BsonDocument, List<BsonDocument>, BsonDocument?, BsonValue> evaluator)
    {
        _accumulatorEvaluator = evaluator;
    }

    internal static BsonValue EvalAccumulator(BsonDocument accSpec, List<BsonDocument> groupDocs, BsonDocument? variables)
    {
        if (_accumulatorEvaluator != null)
            return _accumulatorEvaluator(accSpec, groupDocs, variables);

        throw new NotSupportedException(
            "$accumulator requires the InMemoryEmulator.MongoDB.JsTriggers package for JavaScript execution. " +
            "Install the package and call JsExpressionSetup.Register() to enable $accumulator support.");
    }

    #endregion
}
