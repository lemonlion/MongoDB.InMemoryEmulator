using global::MongoDB.Bson;

namespace InMemoryEmulator.MongoDB;

/// <summary>
/// Evaluates MongoDB projection specifications against BsonDocuments.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/reference/operator/projection/
///   "Projections determine which fields are returned in the matching documents."
///   Inclusion and exclusion cannot be mixed (except _id: 0).
///   _id is included by default unless explicitly excluded.
/// </remarks>
internal static class BsonProjectionEvaluator
{
    /// <summary>
    /// Applies a projection to a document.
    /// </summary>
    /// <param name="doc">The source document.</param>
    /// <param name="projection">Rendered projection, e.g. { name: 1, _id: 0 } or { password: 0 }.</param>
    /// <returns>The projected document.</returns>
    internal static BsonDocument Apply(BsonDocument doc, BsonDocument? projection)
    {
        if (projection == null || projection.ElementCount == 0)
            return doc;

        ValidateProjectionMode(projection);
        var mode = DetermineMode(projection);

        if (mode == ProjectionMode.Inclusion)
            return ApplyInclusion(doc, projection);
        else
            return ApplyExclusion(doc, projection);
    }

    private enum ProjectionMode
    {
        Inclusion,
        Exclusion
    }

    /// <summary>
    /// Validates that a projection does not mix inclusion and exclusion (except _id).
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/method/db.collection.find/#std-label-find-projection
    ///   "You cannot combine inclusion and exclusion statements, with the exception of the _id field."
    /// </remarks>
    private static void ValidateProjectionMode(BsonDocument projection)
    {
        bool? isInclusion = null;
        foreach (var element in projection)
        {
            if (element.Name == "_id") continue;

            bool elementIsInclusion;
            if (element.Value.IsBsonDocument)
            {
                var opDoc = element.Value.AsBsonDocument;
                // $slice is mode-neutral; $elemMatch/$meta are inclusion
                if (opDoc.Contains("$slice"))
                    continue;
                elementIsInclusion = true;
            }
            else
            {
                var val = element.Value.IsNumeric ? element.Value.ToInt32() : (element.Value.AsBoolean ? 1 : 0);
                elementIsInclusion = val == 1;
            }

            if (isInclusion == null)
                isInclusion = elementIsInclusion;
            else if (isInclusion != elementIsInclusion)
                throw MongoErrors.BadValue(
                    $"Cannot do {(elementIsInclusion ? "inclusion" : "exclusion")} on field {element.Name} in {(isInclusion.Value ? "inclusion" : "exclusion")} projection");
        }
    }

    /// <summary>
    /// Determines whether the projection is inclusion or exclusion.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/method/db.collection.find/#std-label-find-projection
    ///   "You cannot combine inclusion and exclusion statements, with the exception of the _id field."
    /// </remarks>
    private static ProjectionMode DetermineMode(BsonDocument projection)
    {
        foreach (var element in projection)
        {
            if (element.Name == "_id") continue;

            if (element.Value.IsBsonDocument)
            {
                var opDoc = element.Value.AsBsonDocument;
                // Ref: https://www.mongodb.com/docs/manual/reference/operator/projection/slice/
                //   "The $slice projection by itself is considered an exclusion."
                //   $slice alone doesn't force inclusion mode; only $elemMatch/$meta do.
                if (opDoc.Contains("$slice"))
                    continue; // $slice is mode-neutral, check other fields
                // $elemMatch and $meta force inclusion mode
                return ProjectionMode.Inclusion;
            }

            var val = element.Value.IsNumeric ? element.Value.ToInt32() : (element.Value.AsBoolean ? 1 : 0);
            return val == 1 ? ProjectionMode.Inclusion : ProjectionMode.Exclusion;
        }

        // Only _id projection or only $slice — treat as exclusion
        if (projection.Contains("_id"))
        {
            var idVal = projection["_id"].IsNumeric ? projection["_id"].ToInt32() : (projection["_id"].AsBoolean ? 1 : 0);
            return idVal == 0 ? ProjectionMode.Exclusion : ProjectionMode.Inclusion;
        }

        // Only $slice projections with no other specs → exclusion mode
        return ProjectionMode.Exclusion;
    }

    /// <summary>
    /// In inclusion mode, only listed fields are included. _id is included by default.
    /// </summary>
    private static BsonDocument ApplyInclusion(BsonDocument doc, BsonDocument projection)
    {
        var result = new BsonDocument();

        // _id handling: included by default unless explicitly excluded
        bool includeId = true;
        if (projection.Contains("_id"))
        {
            var idVal = projection["_id"];
            includeId = idVal.IsNumeric ? idVal.ToInt32() != 0 : idVal.AsBoolean;
        }

        if (includeId && doc.Contains("_id"))
            result["_id"] = doc["_id"];

        foreach (var element in projection)
        {
            if (element.Name == "_id") continue;

            if (element.Value.IsBsonDocument)
            {
                // Handle projection operators ($slice, $elemMatch, $meta)
                ApplyProjectionOperator(doc, result, element.Name, element.Value.AsBsonDocument);
                continue;
            }

            var val = element.Value.IsNumeric ? element.Value.ToInt32() : (element.Value.AsBoolean ? 1 : 0);
            if (val == 1)
            {
                CopyField(doc, result, element.Name);
            }
        }

        return result;
    }

    /// <summary>
    /// In exclusion mode, all fields are included except listed ones.
    /// Supports dot-notation for nested field exclusion.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/tutorial/project-fields-from-query-results/
    ///   "Use dot notation to suppress fields in embedded documents."
    /// </remarks>
    private static BsonDocument ApplyExclusion(BsonDocument doc, BsonDocument projection)
    {
        var excludedFields = new HashSet<string>();
        var sliceFields = new Dictionary<string, BsonValue>();
        foreach (var element in projection)
        {
            if (element.Value.IsBsonDocument)
            {
                var opDoc = element.Value.AsBsonDocument;
                if (opDoc.Contains("$slice"))
                    sliceFields[element.Name] = opDoc["$slice"];
                continue;
            }
            var val = element.Value.IsNumeric ? element.Value.ToInt32() : (element.Value.AsBoolean ? 1 : 0);
            if (val == 0)
                excludedFields.Add(element.Name);
        }

        // Separate top-level exclusions from dot-notation exclusions
        var topLevel = new HashSet<string>();
        var dotNotation = new List<string>();
        foreach (var field in excludedFields)
        {
            if (field.Contains('.'))
                dotNotation.Add(field);
            else
                topLevel.Add(field);
        }

        var result = new BsonDocument();
        foreach (var element in doc)
        {
            if (topLevel.Contains(element.Name))
                continue;
            result[element.Name] = element.Value;
        }

        // Apply dot-notation exclusions to nested fields
        foreach (var field in dotNotation)
        {
            RemoveNestedField(result, field);
        }

        // Apply $slice operators in exclusion mode
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/projection/slice/
        //   "$slice by itself is considered an exclusion" — all fields returned, array sliced.
        foreach (var (field, sliceSpec) in sliceFields)
        {
            ApplySlice(doc, result, field, sliceSpec);
        }

        return result;
    }

    /// <summary>
    /// Removes a nested field specified by dot-notation path.
    /// Handles arrays: when a path segment refers to an array, the field is removed
    /// from each element of the array.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/tutorial/project-fields-from-query-results/
    ///   "You can use dot notation to suppress fields in embedded documents."
    ///   When exclusion path traverses an array, the field is removed from each array element.
    /// </remarks>
    private static void RemoveNestedField(BsonDocument doc, string dotPath)
    {
        var parts = dotPath.Split('.');
        RemoveNestedFieldRecursive(doc, parts, 0);
    }

    private static void RemoveNestedFieldRecursive(BsonDocument doc, string[] parts, int index)
    {
        if (index == parts.Length - 1)
        {
            doc.Remove(parts[index]);
            return;
        }

        var part = parts[index];
        if (!doc.Contains(part))
            return;

        var value = doc[part];
        if (value.IsBsonDocument)
        {
            RemoveNestedFieldRecursive(value.AsBsonDocument, parts, index + 1);
        }
        else if (value.IsBsonArray)
        {
            foreach (var elem in value.AsBsonArray)
            {
                if (elem.IsBsonDocument)
                    RemoveNestedFieldRecursive(elem.AsBsonDocument, parts, index + 1);
            }
        }
    }

    /// <summary>
    /// Copies a field (including dot-notation nested paths) from source to target.
    /// Handles arrays: when a path segment refers to an array, the remaining path
    /// is projected from each element of the array.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/tutorial/project-fields-from-query-results/
    ///   "You can use dot notation to project specific fields inside documents embedded in an array."
    /// </remarks>
    private static void CopyField(BsonDocument source, BsonDocument target, string fieldPath)
    {
        if (!fieldPath.Contains('.'))
        {
            if (source.Contains(fieldPath))
                target[fieldPath] = source[fieldPath];
            return;
        }

        // Dot notation: "address.city" → copy nested path
        var parts = fieldPath.Split('.');
        CopyFieldRecursive(source, target, parts, 0);
    }

    private static void CopyFieldRecursive(BsonDocument source, BsonDocument target, string[] parts, int index)
    {
        if (index == parts.Length - 1)
        {
            // Last part: copy the value
            var lastPart = parts[index];
            if (source.Contains(lastPart))
                target[lastPart] = source[lastPart];
            return;
        }

        var part = parts[index];
        if (!source.Contains(part))
            return;

        var value = source[part];
        if (value.IsBsonDocument)
        {
            if (!target.Contains(part) || !target[part].IsBsonDocument)
                target[part] = new BsonDocument();
            CopyFieldRecursive(value.AsBsonDocument, target[part].AsBsonDocument, parts, index + 1);
        }
        else if (value.IsBsonArray)
        {
            // Ref: https://www.mongodb.com/docs/manual/tutorial/project-fields-from-query-results/
            //   When a dot-notation path traverses an array, the projection applies to each element.
            var sourceArray = value.AsBsonArray;
            var targetArray = new BsonArray();
            var remainingParts = parts.Skip(index + 1).ToArray();
            foreach (var elem in sourceArray)
            {
                if (elem.IsBsonDocument)
                {
                    var projectedElem = new BsonDocument();
                    CopyFieldRecursive(elem.AsBsonDocument, projectedElem, remainingParts, 0);
                    targetArray.Add(projectedElem);
                }
                else
                {
                    targetArray.Add(elem);
                }
            }
            target[part] = targetArray;
        }
    }

    /// <summary>
    /// Handles projection operators ($slice, $elemMatch, $meta).
    /// </summary>
    private static void ApplyProjectionOperator(BsonDocument doc, BsonDocument result, string field, BsonDocument operatorDoc)
    {
        if (operatorDoc.Contains("$slice"))
        {
            ApplySlice(doc, result, field, operatorDoc["$slice"]);
        }
        else if (operatorDoc.Contains("$elemMatch"))
        {
            ApplyElemMatch(doc, result, field, operatorDoc["$elemMatch"].AsBsonDocument);
        }
        else if (operatorDoc.Contains("$meta"))
        {
            // $meta: "textScore" — stub: include field with 0.0 score
            result[field] = 0.0;
        }
        else
        {
            // Unknown operator — just include the field
            if (doc.Contains(field))
                result[field] = doc[field];
        }
    }

    /// <summary>
    /// $slice projection: returns a subset of an array.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/operator/projection/slice/
    ///   { field: { $slice: N } } — first N elements (positive) or last N (negative)
    ///   { field: { $slice: [skip, limit] } } — skip elements, then take limit
    /// </remarks>
    private static void ApplySlice(BsonDocument doc, BsonDocument result, string field, BsonValue sliceSpec)
    {
        // Support dot-notation: resolve path through nested documents
        var arrayValue = BsonFilterEvaluator.ResolveFieldPath(doc, field);
        if (arrayValue == BsonNull.Value || !arrayValue.IsBsonArray)
        {
            // If field exists but isn't array, include as-is
            if (arrayValue != BsonNull.Value && field.Contains('.'))
                SetNestedField(result, field, arrayValue);
            else if (doc.Contains(field))
                result[field] = doc[field];
            return;
        }

        var array = arrayValue.AsBsonArray;
        BsonArray sliced;

        if (sliceSpec.IsNumeric)
        {
            var n = sliceSpec.ToInt32();
            if (n >= 0)
                sliced = new BsonArray(array.Take(n));
            else
                sliced = new BsonArray(array.Skip(Math.Max(0, array.Count + n)));
        }
        else if (sliceSpec.IsBsonArray)
        {
            var arr = sliceSpec.AsBsonArray;
            // The MongoDB driver renders $slice in aggregation expression format:
            //   { $slice: ["$field", count] } or { $slice: ["$field", skip, count] }
            // Legacy format: { $slice: [skip, count] } where both are integers
            if (arr.Count >= 2 && arr[0].IsString && arr[0].AsString.StartsWith("$"))
            {
                // Aggregation expression format: ["$field", count] or ["$field", skip, count]
                if (arr.Count == 2)
                {
                    var n = arr[1].ToInt32();
                    if (n >= 0)
                        sliced = new BsonArray(array.Take(n));
                    else
                        sliced = new BsonArray(array.Skip(Math.Max(0, array.Count + n)));
                }
                else
                {
                    // ["$field", skip, count]
                    var skip = arr[1].ToInt32();
                    var limit = arr[2].ToInt32();
                    if (skip < 0)
                        skip = Math.Max(0, array.Count + skip);
                    sliced = new BsonArray(array.Skip(skip).Take(limit));
                }
            }
            else
            {
                // Legacy format: [skip, count]
                var skip = arr[0].ToInt32();
                var limit = arr[1].ToInt32();
                if (skip < 0)
                    skip = Math.Max(0, array.Count + skip);
                sliced = new BsonArray(array.Skip(skip).Take(limit));
            }
        }
        else
        {
            return;
        }

        // Set the sliced array at the correct path
        if (field.Contains('.'))
            SetNestedField(result, field, sliced);
        else
            result[field] = sliced;
    }

    /// <summary>
    /// $elemMatch projection: returns only the first matching array element.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/operator/projection/elemMatch/
    ///   "Projects the first element in an array that matches the specified $elemMatch condition."
    /// </remarks>
    private static void ApplyElemMatch(BsonDocument doc, BsonDocument result, string field, BsonDocument filter)
    {
        // Support dot-notation: resolve path through nested documents
        var arrayValue = BsonFilterEvaluator.ResolveFieldPath(doc, field);
        if (arrayValue == BsonNull.Value || !arrayValue.IsBsonArray)
            return;

        var array = arrayValue.AsBsonArray;
        foreach (var element in array)
        {
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/projection/elemMatch/
            //   "Projects the first element in an array that matches the specified $elemMatch condition."
            //   Works with both document and scalar array elements.
            if (element.IsBsonDocument)
            {
                if (BsonFilterEvaluator.Matches(element.AsBsonDocument, filter))
                {
                    var matched = new BsonArray { element };
                    if (field.Contains('.'))
                        SetNestedField(result, field, matched);
                    else
                        result[field] = matched;
                    return;
                }
            }
            else
            {
                // For scalar elements, wrap in a temporary document and match against the filter
                // treating the filter operators as applying to the element value directly.
                var wrapper = new BsonDocument("_v", element);
                var wrappedFilter = new BsonDocument("_v", filter);
                if (BsonFilterEvaluator.Matches(wrapper, wrappedFilter))
                {
                    var matched = new BsonArray { element };
                    if (field.Contains('.'))
                        SetNestedField(result, field, matched);
                    else
                        result[field] = matched;
                    return;
                }
            }
        }

        // No match — field not included
    }

    /// <summary>
    /// Sets a value at a dot-notation path in the result document, creating intermediate documents as needed.
    /// </summary>
    private static void SetNestedField(BsonDocument doc, string dotPath, BsonValue value)
    {
        var parts = dotPath.Split('.');
        var current = doc;
        for (int i = 0; i < parts.Length - 1; i++)
        {
            if (!current.Contains(parts[i]) || !current[parts[i]].IsBsonDocument)
                current[parts[i]] = new BsonDocument();
            current = current[parts[i]].AsBsonDocument;
        }
        current[parts[^1]] = value;
    }
}
