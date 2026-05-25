using global::MongoDB.Bson;

namespace InMemoryEmulator.MongoDB;

/// <summary>
/// Comparer for BsonValue that follows MongoDB's comparison/sort order.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/
///   "MongoDB uses the following comparison order for BSON types from lowest to highest:
///    MinKey, Null, Numbers, Symbol, String, Object, Array, BinData, ObjectId, Boolean, Date,
///    Timestamp, Regular Expression, MaxKey."
/// </remarks>
internal sealed class BsonValueComparer : IEqualityComparer<BsonValue>, IComparer<BsonValue>
{
    public static readonly BsonValueComparer Instance = new();

    public bool Equals(BsonValue? x, BsonValue? y)
    {
        if (x is null && y is null) return true;
        if (x is null || y is null) return false;
        // Ref: https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/
        //   Numbers (ints, longs, doubles, decimals) compare cross-type by value.
        if (IsNumeric(x) && IsNumeric(y))
            return x.CompareTo(y) == 0;
        return x.Equals(y);
    }

    public int GetHashCode(BsonValue obj)
    {
        // Numeric types that represent the same mathematical value must have the same hash code.
        if (IsNumeric(obj))
        {
            var d = obj.ToDouble();
            // For integer values, normalize to long hash to ensure Int32/Int64/Double/Decimal128 match
            if (d == Math.Floor(d) && d >= long.MinValue && d <= long.MaxValue && !double.IsNaN(d))
                return ((long)d).GetHashCode();
            return d.GetHashCode();
        }
        return obj.GetHashCode();
    }

    public int Compare(BsonValue? x, BsonValue? y)
    {
        if (x is null && y is null) return 0;
        if (x is null) return -1;
        if (y is null) return 1;
        return x.CompareTo(y);
    }

    private static bool IsNumeric(BsonValue val)
    {
        return val.BsonType is BsonType.Int32 or BsonType.Int64 or BsonType.Double or BsonType.Decimal128;
    }
}
