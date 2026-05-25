using global::MongoDB.Bson;

namespace InMemoryEmulator.MongoDB;

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

            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/sort/
            //   "{ $meta: 'textScore' }" sort expression — treat as descending (highest score first).
            //   Since we don't compute real text scores, sort by constant (no-op for ordering).
            if (element.Value.IsBsonDocument)
            {
                // $meta sort — skip (text score is always 0.0 in this emulator)
                continue;
            }

            var direction = element.Value.AsInt32; // 1 = ascending, -1 = descending

            if (ordered == null)
            {
                ordered = direction == 1
                    ? docs.OrderBy(d => ResolveFieldForSort(d, field, direction), BsonValueComparer.Instance)
                    : docs.OrderByDescending(d => ResolveFieldForSort(d, field, direction), BsonValueComparer.Instance);
            }
            else
            {
                ordered = direction == 1
                    ? ordered.ThenBy(d => ResolveFieldForSort(d, field, direction), BsonValueComparer.Instance)
                    : ordered.ThenByDescending(d => ResolveFieldForSort(d, field, direction), BsonValueComparer.Instance);
            }
        }

        return ordered?.ToList() ?? docs.ToList();
    }

    /// <summary>
    /// Resolves a dot-notation field path to a BsonValue for sorting purposes.
    /// For array fields, extracts the minimum element (ascending) or maximum element (descending).
    /// Returns BsonNull for missing fields (sorts missing as lowest value).
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/method/cursor.sort/#sort-asc-desc
    ///   "For an ascending sort, comparison of a multi-value field such as an array
    ///    to a single value field in another document, the sort picks the least value
    ///    of the multi-value field for comparison."
    ///   "For a descending sort, comparison treats the multi-value field as the greatest
    ///    value in the field."
    /// </remarks>
    private static BsonValue ResolveFieldForSort(BsonDocument doc, string path, int direction)
    {
        var value = BsonFilterEvaluator.ResolveFieldPath(doc, path);

        // Ref: https://www.mongodb.com/docs/manual/core/document/#dot-notation
        //   Dot-notation traverses arrays of subdocuments. Collect all values
        //   reached through array traversal to use for multikey sort comparison.
        if (value == BsonNull.Value && path.Contains('.'))
        {
            var allValues = BsonFilterEvaluator.ResolveFieldPathThroughArrays(doc, path);
            // Filter out BsonNull placeholders from missing sub-paths
            var nonNull = allValues.Where(v => v != BsonNull.Value && !v.IsBsonNull).ToList();
            if (nonNull.Count > 0)
            {
                return direction == 1
                    ? nonNull.OrderBy(x => x, BsonValueComparer.Instance).First()
                    : nonNull.OrderByDescending(x => x, BsonValueComparer.Instance).First();
            }
            return BsonNull.Value;
        }

        if (value is BsonArray array && array.Count > 0)
        {
            return direction == 1
                ? array.OrderBy(x => x, BsonValueComparer.Instance).First()
                : array.OrderByDescending(x => x, BsonValueComparer.Instance).First();
        }

        return value;
    }
}
