using MongoDB.Bson;

namespace MongoDB.InMemoryEmulator;

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
        return x.Equals(y);
    }

    public int GetHashCode(BsonValue obj)
    {
        return obj.GetHashCode();
    }

    public int Compare(BsonValue? x, BsonValue? y)
    {
        if (x is null && y is null) return 0;
        if (x is null) return -1;
        if (y is null) return 1;
        return x.CompareTo(y);
    }
}
