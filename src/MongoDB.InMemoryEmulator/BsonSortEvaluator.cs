using MongoDB.Bson;

namespace MongoDB.InMemoryEmulator;

/// <summary>
/// Evaluates MongoDB sort specifications against BsonDocument collections.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/reference/method/cursor.sort/
///   "Specifies the order in which the query returns matching documents.
///    You must apply sort() to the cursor before retrieving any documents."
/// </remarks>
internal static class BsonSortEvaluator
{
    /// <summary>
    /// Sorts documents according to a rendered sort specification.
    /// </summary>
    /// <param name="docs">Documents to sort.</param>
    /// <param name="sort">Rendered sort document, e.g. { "date": -1, "name": 1 }.</param>
    /// <returns>Sorted documents.</returns>
    internal static IReadOnlyList<BsonDocument> Apply(IEnumerable<BsonDocument> docs, BsonDocument? sort)
    {
        if (sort == null || sort.ElementCount == 0)
            return docs as IReadOnlyList<BsonDocument> ?? docs.ToList();

        IOrderedEnumerable<BsonDocument>? ordered = null;

        foreach (var element in sort)
        {
            var field = element.Name;
            var direction = element.Value.AsInt32; // 1 = ascending, -1 = descending

            if (ordered == null)
            {
                ordered = direction == 1
                    ? docs.OrderBy(d => ResolveField(d, field), BsonValueComparer.Instance)
                    : docs.OrderByDescending(d => ResolveField(d, field), BsonValueComparer.Instance);
            }
            else
            {
                ordered = direction == 1
                    ? ordered.ThenBy(d => ResolveField(d, field), BsonValueComparer.Instance)
                    : ordered.ThenByDescending(d => ResolveField(d, field), BsonValueComparer.Instance);
            }
        }

        return ordered?.ToList() ?? docs.ToList();
    }

    /// <summary>
    /// Resolves a dot-notation field path to a BsonValue.
    /// Returns BsonNull for missing fields (sorts missing as lowest value).
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/sort/
    ///   "When comparing values of different BSON types, MongoDB uses the following
    ///    comparison order: MinKey < Null < Numbers < Symbol < String < Object < Array
    ///    < BinData < ObjectId < Boolean < Date < Timestamp < RegularExpression < MaxKey"
    /// </remarks>
    private static BsonValue ResolveField(BsonDocument doc, string path)
    {
        return BsonFilterEvaluator.ResolveFieldPath(doc, path);
    }
}
