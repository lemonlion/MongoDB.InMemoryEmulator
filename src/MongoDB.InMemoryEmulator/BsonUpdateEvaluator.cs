using MongoDB.Bson;

namespace MongoDB.InMemoryEmulator;

/// <summary>
/// Evaluates MongoDB update operators against BsonDocuments.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/reference/operator/update/
///   "Update operators modify the values of fields in a document."
/// </remarks>
internal static class BsonUpdateEvaluator
{
    /// <summary>
    /// Applies update operators to a document. Returns the modified document.
    /// The document is deep-cloned before modification.
    /// </summary>
    /// <param name="document">The original document.</param>
    /// <param name="update">The rendered update document with $ operators.</param>
    /// <param name="arrayFilters">Optional array filters for positional filtered updates.</param>
    /// <param name="isUpsertInsert">True if this is the insert portion of an upsert.</param>
    /// <returns>The updated document.</returns>
    internal static BsonDocument Apply(BsonDocument document, BsonDocument update,
        IReadOnlyList<BsonDocument>? arrayFilters = null, bool isUpsertInsert = false)
    {
        var result = document.DeepClone().AsBsonDocument;

        foreach (var element in update)
        {
            switch (element.Name)
            {
                // Ref: https://www.mongodb.com/docs/manual/reference/operator/update-field/
                case "$set":
                    ValidateIdNotTargeted(element.Value.AsBsonDocument, "$set");
                    ApplySet(result, element.Value.AsBsonDocument);
                    break;
                case "$unset":
                    ValidateIdNotTargeted(element.Value.AsBsonDocument, "$unset");
                    ApplyUnset(result, element.Value.AsBsonDocument);
                    break;
                case "$inc":
                    ValidateIdNotTargeted(element.Value.AsBsonDocument, "$inc");
                    ApplyInc(result, element.Value.AsBsonDocument);
                    break;
                case "$mul":
                    ValidateIdNotTargeted(element.Value.AsBsonDocument, "$mul");
                    ApplyMul(result, element.Value.AsBsonDocument);
                    break;
                case "$min":
                    ValidateIdNotTargeted(element.Value.AsBsonDocument, "$min");
                    ApplyMin(result, element.Value.AsBsonDocument);
                    break;
                case "$max":
                    ValidateIdNotTargeted(element.Value.AsBsonDocument, "$max");
                    ApplyMax(result, element.Value.AsBsonDocument);
                    break;
                case "$rename":
                    ValidateRenameNotTargetingId(element.Value.AsBsonDocument);
                    ApplyRename(result, element.Value.AsBsonDocument);
                    break;
                case "$currentDate":
                    ValidateIdNotTargeted(element.Value.AsBsonDocument, "$currentDate");
                    ApplyCurrentDate(result, element.Value.AsBsonDocument);
                    break;
                case "$setOnInsert":
                    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/setOnInsert/
                    //   "Only applied when an upsert inserts a new document."
                    if (isUpsertInsert)
                        ApplySet(result, element.Value.AsBsonDocument);
                    break;

                // Ref: https://www.mongodb.com/docs/manual/reference/operator/update-array/
                case "$push":
                    ApplyPush(result, element.Value.AsBsonDocument);
                    break;
                case "$pull":
                    ApplyPull(result, element.Value.AsBsonDocument);
                    break;
                case "$pullAll":
                    ApplyPullAll(result, element.Value.AsBsonDocument);
                    break;
                case "$addToSet":
                    ApplyAddToSet(result, element.Value.AsBsonDocument);
                    break;
                case "$pop":
                    ApplyPop(result, element.Value.AsBsonDocument);
                    break;

                // Ref: https://www.mongodb.com/docs/manual/reference/operator/update-bitwise/
                case "$bit":
                    ApplyBit(result, element.Value.AsBsonDocument);
                    break;

                default:
                    if (!element.Name.StartsWith("$"))
                        throw MongoErrors.FailedToParse(
                            $"Unknown modifier: {element.Name}. Expected a valid update modifier or pipeline-style update.");
                    break;
            }
        }

        return result;
    }

    /// <summary>
    /// Validates that the update document contains only $ operators (not a replacement).
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/command/update/
    ///   "the update operation document must contain atomic operators"
    /// </remarks>
    internal static void ValidateIsUpdateDocument(BsonDocument update)
    {
        if (!update.Names.Any(n => n.StartsWith("$")))
            throw MongoErrors.FailedToParse(
                "the update operation document must contain atomic operators");
    }

    #region Field Update Operators

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/set/
    //   "Sets the value of a field in a document."
    private static void ApplySet(BsonDocument doc, BsonDocument fields)
    {
        foreach (var element in fields)
        {
            SetFieldPath(doc, element.Name, element.Value);
        }
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/unset/
    //   "Removes the specified field from a document."
    private static void ApplyUnset(BsonDocument doc, BsonDocument fields)
    {
        foreach (var element in fields)
        {
            RemoveFieldPath(doc, element.Name);
        }
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/inc/
    //   "Increments the value of the field by the specified amount."
    private static void ApplyInc(BsonDocument doc, BsonDocument fields)
    {
        foreach (var element in fields)
        {
            var current = ResolveFieldPath(doc, element.Name);
            var increment = element.Value;
            var newValue = AddBsonValues(current, increment);
            SetFieldPath(doc, element.Name, newValue);
        }
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/mul/
    //   "Multiplies the value of the field by the specified amount."
    private static void ApplyMul(BsonDocument doc, BsonDocument fields)
    {
        foreach (var element in fields)
        {
            var current = ResolveFieldPath(doc, element.Name);
            if (current == BsonNull.Value) current = new BsonInt32(0);
            var multiplier = element.Value;
            var newValue = MultiplyBsonValues(current, multiplier);
            SetFieldPath(doc, element.Name, newValue);
        }
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/min/
    //   "Updates the value of the field to the specified value if the specified value
    //    is less than the current value of the field."
    private static void ApplyMin(BsonDocument doc, BsonDocument fields)
    {
        foreach (var element in fields)
        {
            var current = ResolveFieldPath(doc, element.Name);
            if (current == BsonNull.Value || BsonValueComparer.Instance.Compare(element.Value, current) < 0)
                SetFieldPath(doc, element.Name, element.Value);
        }
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/max/
    //   "Updates the value of the field to the specified value if the specified value
    //    is greater than the current value of the field."
    private static void ApplyMax(BsonDocument doc, BsonDocument fields)
    {
        foreach (var element in fields)
        {
            var current = ResolveFieldPath(doc, element.Name);
            if (current == BsonNull.Value || BsonValueComparer.Instance.Compare(element.Value, current) > 0)
                SetFieldPath(doc, element.Name, element.Value);
        }
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/rename/
    //   "Renames a field."
    private static void ApplyRename(BsonDocument doc, BsonDocument fields)
    {
        foreach (var element in fields)
        {
            var oldName = element.Name;
            var newName = element.Value.AsString;
            var value = ResolveFieldPath(doc, oldName);
            if (value != BsonNull.Value)
            {
                RemoveFieldPath(doc, oldName);
                SetFieldPath(doc, newName, value);
            }
        }
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/currentDate/
    //   "Sets the value of a field to current date, either as a Date or a Timestamp."
    private static void ApplyCurrentDate(BsonDocument doc, BsonDocument fields)
    {
        foreach (var element in fields)
        {
            if (element.Value.IsBsonDocument)
            {
                var spec = element.Value.AsBsonDocument;
                if (spec.Contains("$type") && spec["$type"].AsString == "timestamp")
                {
                    SetFieldPath(doc, element.Name, new BsonTimestamp((int)DateTimeOffset.UtcNow.ToUnixTimeSeconds(), 1));
                    continue;
                }
            }
            SetFieldPath(doc, element.Name, new BsonDateTime(DateTime.UtcNow));
        }
    }

    #endregion

    #region Array Update Operators

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/push/
    //   "Appends a specified value to an array."
    private static void ApplyPush(BsonDocument doc, BsonDocument fields)
    {
        foreach (var element in fields)
        {
            var fieldPath = element.Name;
            var current = ResolveFieldPath(doc, fieldPath);

            if (current == BsonNull.Value)
            {
                SetFieldPath(doc, fieldPath, new BsonArray());
                current = ResolveFieldPath(doc, fieldPath);
            }

            if (current is not BsonArray array)
                throw MongoErrors.BadValue($"The field '{fieldPath}' must be an array but is of type {current.BsonType}");

            if (element.Value.IsBsonDocument && element.Value.AsBsonDocument.Contains("$each"))
            {
                ApplyPushEach(array, element.Value.AsBsonDocument);
            }
            else
            {
                array.Add(element.Value);
            }
        }
    }

    private static void ApplyPushEach(BsonArray array, BsonDocument spec)
    {
        var items = spec["$each"].AsBsonArray;
        var position = spec.Contains("$position") ? spec["$position"].ToInt32() : -1;

        if (position >= 0)
        {
            for (int i = 0; i < items.Count; i++)
                array.Insert(Math.Min(position + i, array.Count), items[i]);
        }
        else
        {
            foreach (var item in items)
                array.Add(item);
        }

        // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/push/#std-label-push-each-modifier
        //   "$sort sorts the modified array after $each."
        if (spec.Contains("$sort"))
        {
            var sortSpec = spec["$sort"];
            BsonArray sorted;

            if (sortSpec.IsNumeric)
            {
                sorted = sortSpec.ToInt32() == 1
                    ? new BsonArray(array.OrderBy(x => x, BsonValueComparer.Instance))
                    : new BsonArray(array.OrderByDescending(x => x, BsonValueComparer.Instance));
            }
            else if (sortSpec.IsBsonDocument)
            {
                var sortDoc = sortSpec.AsBsonDocument;
                var items2 = array.Select(x => x.AsBsonDocument).ToList();
                var sortedDocs = BsonSortEvaluator.Apply(items2, sortDoc);
                sorted = new BsonArray(sortedDocs);
            }
            else
            {
                sorted = array;
            }

            array.Clear();
            foreach (var item in sorted) array.Add(item);
        }

        // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/push/#std-label-push-each-modifier
        //   "$slice limits the number of array elements."
        if (spec.Contains("$slice"))
        {
            var sliceVal = spec["$slice"].ToInt32();
            if (sliceVal >= 0)
            {
                while (array.Count > sliceVal)
                    array.RemoveAt(array.Count - 1);
            }
            else
            {
                while (array.Count > -sliceVal)
                    array.RemoveAt(0);
            }
        }
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/pull/
    //   "Removes all array elements that match a specified query."
    private static void ApplyPull(BsonDocument doc, BsonDocument fields)
    {
        foreach (var element in fields)
        {
            var current = ResolveFieldPath(doc, element.Name);
            if (current is not BsonArray array) continue;

            var condition = element.Value;
            var toRemove = new List<int>();
            for (int i = 0; i < array.Count; i++)
            {
                if (ShouldPull(array[i], condition))
                    toRemove.Add(i);
            }

            for (int i = toRemove.Count - 1; i >= 0; i--)
                array.RemoveAt(toRemove[i]);
        }
    }

    private static bool ShouldPull(BsonValue element, BsonValue condition)
    {
        if (condition.IsBsonDocument)
        {
            var condDoc = condition.AsBsonDocument;
            if (condDoc.Names.Any(n => n.StartsWith("$")))
            {
                // Condition is a query expression
                if (element.IsBsonDocument)
                    return BsonFilterEvaluator.Matches(element.AsBsonDocument, condDoc);
                // Apply condition as scalar comparison
                var wrapDoc = new BsonDocument("_v", element);
                var wrapFilter = new BsonDocument("_v", condition);
                return BsonFilterEvaluator.Matches(wrapDoc, wrapFilter);
            }
            // Match exact document
            return element.Equals(condition);
        }
        return element.Equals(condition);
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/pullAll/
    //   "Removes all instances of the specified values from an existing array."
    private static void ApplyPullAll(BsonDocument doc, BsonDocument fields)
    {
        foreach (var element in fields)
        {
            var current = ResolveFieldPath(doc, element.Name);
            if (current is not BsonArray array) continue;

            var valuesToRemove = element.Value.AsBsonArray;
            for (int i = array.Count - 1; i >= 0; i--)
            {
                if (valuesToRemove.Any(v => v.Equals(array[i])))
                    array.RemoveAt(i);
            }
        }
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/addToSet/
    //   "Adds a value to an array unless the value is already present."
    private static void ApplyAddToSet(BsonDocument doc, BsonDocument fields)
    {
        foreach (var element in fields)
        {
            var current = ResolveFieldPath(doc, element.Name);
            if (current == BsonNull.Value)
            {
                SetFieldPath(doc, element.Name, new BsonArray());
                current = ResolveFieldPath(doc, element.Name);
            }

            if (current is not BsonArray array)
                throw MongoErrors.BadValue($"The field '{element.Name}' must be an array");

            if (element.Value.IsBsonDocument && element.Value.AsBsonDocument.Contains("$each"))
            {
                var items = element.Value.AsBsonDocument["$each"].AsBsonArray;
                foreach (var item in items)
                {
                    if (!array.Any(x => x.Equals(item)))
                        array.Add(item);
                }
            }
            else
            {
                if (!array.Any(x => x.Equals(element.Value)))
                    array.Add(element.Value);
            }
        }
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/pop/
    //   "Removes the first or last element of an array."
    private static void ApplyPop(BsonDocument doc, BsonDocument fields)
    {
        foreach (var element in fields)
        {
            var current = ResolveFieldPath(doc, element.Name);
            if (current is not BsonArray array || array.Count == 0) continue;

            if (element.Value.ToInt32() == -1)
                array.RemoveAt(0);
            else
                array.RemoveAt(array.Count - 1);
        }
    }

    #endregion

    #region Bitwise Update Operator

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/bit/
    //   "Performs a bitwise AND, OR, or XOR update of a field."
    private static void ApplyBit(BsonDocument doc, BsonDocument fields)
    {
        foreach (var element in fields)
        {
            var current = ResolveFieldPath(doc, element.Name);
            long currentVal = current.IsInt64 ? current.AsInt64 : current.IsInt32 ? current.AsInt32 : 0;

            var ops = element.Value.AsBsonDocument;
            foreach (var op in ops)
            {
                long operand = op.Value.IsInt64 ? op.Value.AsInt64 : op.Value.AsInt32;
                currentVal = op.Name switch
                {
                    "and" => currentVal & operand,
                    "or" => currentVal | operand,
                    "xor" => currentVal ^ operand,
                    _ => throw MongoErrors.BadValue($"Unknown bit operation: {op.Name}")
                };
            }

            if (current.IsInt64)
                SetFieldPath(doc, element.Name, new BsonInt64(currentVal));
            else
                SetFieldPath(doc, element.Name, new BsonInt32((int)currentVal));
        }
    }

    #endregion

    #region Validation

    private static void ValidateIdNotTargeted(BsonDocument fields, string operatorName)
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/error-codes/
        //   Error code 66: ImmutableField — "_id" cannot be modified after creation
        if (fields.Contains("_id"))
            throw MongoErrors.ImmutableField("_id");
    }

    private static void ValidateRenameNotTargetingId(BsonDocument renames)
    {
        foreach (var element in renames)
        {
            if (element.Name == "_id")
                throw MongoErrors.ImmutableField("_id");
            if (element.Value.AsString == "_id")
                throw MongoErrors.ImmutableField("_id");
        }
    }

    #endregion

    #region Helpers

    private static BsonValue ResolveFieldPath(BsonDocument doc, string path)
    {
        return BsonFilterEvaluator.ResolveFieldPath(doc, path);
    }

    /// <summary>
    /// Sets a value at a dot-notation path, creating intermediate documents as needed.
    /// </summary>
    internal static void SetFieldPath(BsonDocument doc, string path, BsonValue value)
    {
        if (!path.Contains('.'))
        {
            doc[path] = value;
            return;
        }

        var parts = path.Split('.');
        var current = doc;

        for (int i = 0; i < parts.Length - 1; i++)
        {
            if (!current.Contains(parts[i]) || !current[parts[i]].IsBsonDocument)
            {
                current[parts[i]] = new BsonDocument();
            }
            current = current[parts[i]].AsBsonDocument;
        }

        current[parts[^1]] = value;
    }

    private static void RemoveFieldPath(BsonDocument doc, string path)
    {
        if (!path.Contains('.'))
        {
            doc.Remove(path);
            return;
        }

        var parts = path.Split('.');
        var current = doc;

        for (int i = 0; i < parts.Length - 1; i++)
        {
            if (!current.Contains(parts[i]) || !current[parts[i]].IsBsonDocument)
                return;
            current = current[parts[i]].AsBsonDocument;
        }

        current.Remove(parts[^1]);
    }

    private static BsonValue AddBsonValues(BsonValue a, BsonValue b)
    {
        if (a == BsonNull.Value) a = new BsonInt32(0);

        return (a.BsonType, b.BsonType) switch
        {
            (BsonType.Double, _) or (_, BsonType.Double) =>
                new BsonDouble(a.ToDouble() + b.ToDouble()),
            (BsonType.Decimal128, _) or (_, BsonType.Decimal128) =>
                new BsonDecimal128(Decimal128.ToDecimal(a.AsDecimal128) + Decimal128.ToDecimal(b.AsDecimal128)),
            (BsonType.Int64, _) or (_, BsonType.Int64) =>
                new BsonInt64(a.ToInt64() + b.ToInt64()),
            _ =>
                new BsonInt32(a.ToInt32() + b.ToInt32()),
        };
    }

    private static BsonValue MultiplyBsonValues(BsonValue a, BsonValue b)
    {
        return (a.BsonType, b.BsonType) switch
        {
            (BsonType.Double, _) or (_, BsonType.Double) =>
                new BsonDouble(a.ToDouble() * b.ToDouble()),
            (BsonType.Decimal128, _) or (_, BsonType.Decimal128) =>
                new BsonDecimal128(Decimal128.ToDecimal(a.AsDecimal128) * Decimal128.ToDecimal(b.AsDecimal128)),
            (BsonType.Int64, _) or (_, BsonType.Int64) =>
                new BsonInt64(a.ToInt64() * b.ToInt64()),
            _ =>
                new BsonInt32(a.ToInt32() * b.ToInt32()),
        };
    }

    #endregion
}
