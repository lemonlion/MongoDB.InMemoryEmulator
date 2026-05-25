using global::MongoDB.Bson;

namespace InMemoryEmulator.MongoDB;

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
    /// <param name="matchedArrayIndex">Index of the first matched array element for positional $ operator.</param>
    /// <param name="matchedArrayField">Name of the array field that was matched for positional $ operator.</param>
    /// <returns>The updated document.</returns>
    internal static BsonDocument Apply(BsonDocument document, BsonDocument update,
        IReadOnlyList<BsonDocument>? arrayFilters = null, bool isUpsertInsert = false,
        int matchedArrayIndex = -1, string? matchedArrayField = null)
    {
        var result = document.DeepClone().AsBsonDocument;
        var ctx = new PositionalContext(matchedArrayIndex, matchedArrayField, arrayFilters);

        foreach (var element in update)
        {
            switch (element.Name)
            {
                // Ref: https://www.mongodb.com/docs/manual/reference/operator/update-field/
                case "$set":
                    ValidateIdNotTargeted(element.Value.AsBsonDocument, "$set");
                    ApplySet(result, element.Value.AsBsonDocument, ctx);
                    break;
                case "$unset":
                    ValidateIdNotTargeted(element.Value.AsBsonDocument, "$unset");
                    ApplyUnset(result, element.Value.AsBsonDocument, ctx);
                    break;
                case "$inc":
                    ValidateIdNotTargeted(element.Value.AsBsonDocument, "$inc");
                    ApplyInc(result, element.Value.AsBsonDocument, ctx);
                    break;
                case "$mul":
                    ValidateIdNotTargeted(element.Value.AsBsonDocument, "$mul");
                    ApplyMul(result, element.Value.AsBsonDocument, ctx);
                    break;
                case "$min":
                    ValidateIdNotTargeted(element.Value.AsBsonDocument, "$min");
                    ApplyMin(result, element.Value.AsBsonDocument, ctx);
                    break;
                case "$max":
                    ValidateIdNotTargeted(element.Value.AsBsonDocument, "$max");
                    ApplyMax(result, element.Value.AsBsonDocument, ctx);
                    break;
                case "$rename":
                    ValidateRenameNotTargetingId(element.Value.AsBsonDocument);
                    ApplyRename(result, element.Value.AsBsonDocument);
                    break;
                case "$currentDate":
                    ValidateIdNotTargeted(element.Value.AsBsonDocument, "$currentDate");
                    ApplyCurrentDate(result, element.Value.AsBsonDocument, ctx);
                    break;
                case "$setOnInsert":
                    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/setOnInsert/
                    //   "Only applied when an upsert inserts a new document."
                    if (isUpsertInsert)
                        ApplySet(result, element.Value.AsBsonDocument, ctx);
                    break;

                // Ref: https://www.mongodb.com/docs/manual/reference/operator/update-array/
                case "$push":
                    ApplyPush(result, element.Value.AsBsonDocument, ctx);
                    break;
                case "$pull":
                    ApplyPull(result, element.Value.AsBsonDocument, ctx);
                    break;
                case "$pullAll":
                    ApplyPullAll(result, element.Value.AsBsonDocument, ctx);
                    break;
                case "$addToSet":
                    ApplyAddToSet(result, element.Value.AsBsonDocument, ctx);
                    break;
                case "$pop":
                    ApplyPop(result, element.Value.AsBsonDocument, ctx);
                    break;

                // Ref: https://www.mongodb.com/docs/manual/reference/operator/update-bitwise/
                case "$bit":
                    ApplyBit(result, element.Value.AsBsonDocument, ctx);
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
    private static void ApplySet(BsonDocument doc, BsonDocument fields, PositionalContext ctx)
    {
        foreach (var element in fields)
        {
            SetFieldPathPositional(doc, element.Name, element.Value, ctx);
        }
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/unset/
    //   "Removes the specified field from a document."
    private static void ApplyUnset(BsonDocument doc, BsonDocument fields, PositionalContext ctx)
    {
        foreach (var element in fields)
        {
            if (HasPositionalOperator(element.Name))
                ApplyPositionalAction(doc, element.Name, ctx, (d, leaf) => d.Remove(leaf));
            else
                RemoveFieldPath(doc, element.Name);
        }
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/inc/
    //   "Increments the value of the field by the specified amount."
    private static void ApplyInc(BsonDocument doc, BsonDocument fields, PositionalContext ctx)
    {
        foreach (var element in fields)
        {
            if (HasPositionalOperator(element.Name))
            {
                ApplyPositionalAction(doc, element.Name, ctx, (d, leaf) =>
                {
                    var current = d.Contains(leaf) ? d[leaf] : new BsonInt32(0);
                    d[leaf] = AddBsonValues(current, element.Value);
                });
            }
            else
            {
                // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/inc/
                //   If the field does not exist, $inc creates the field and sets it to the specified value.
                //   But if the field exists with a null value, it should throw.
                var fieldExists = BsonFilterEvaluator.FieldExists(doc, element.Name);
                var current = fieldExists ? ResolveFieldPath(doc, element.Name) : new BsonInt32(0);
                var newValue = AddBsonValues(current, element.Value);
                SetFieldPath(doc, element.Name, newValue);
            }
        }
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/mul/
    //   "Multiplies the value of the field by the specified amount."
    private static void ApplyMul(BsonDocument doc, BsonDocument fields, PositionalContext ctx)
    {
        foreach (var element in fields)
        {
            if (HasPositionalOperator(element.Name))
            {
                ApplyPositionalAction(doc, element.Name, ctx, (d, leaf) =>
                {
                    var current = d.Contains(leaf) ? d[leaf] : new BsonInt32(0);
                    d[leaf] = MultiplyBsonValues(current, element.Value);
                });
            }
            else
            {
                // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/mul/
                //   If the field does not exist, $mul creates the field and sets it to 0.
                //   But if the field exists with a null value, it should throw.
                var fieldExists = BsonFilterEvaluator.FieldExists(doc, element.Name);
                var current = fieldExists ? ResolveFieldPath(doc, element.Name) : new BsonInt32(0);
                if (current == BsonNull.Value)
                    throw MongoErrors.BadValue($"Cannot apply $mul to a value of non-numeric type null");
                var newValue = MultiplyBsonValues(current, element.Value);
                SetFieldPath(doc, element.Name, newValue);
            }
        }
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/min/
    //   "Updates the value of the field to the specified value if the specified value
    //    is less than the current value of the field."
    private static void ApplyMin(BsonDocument doc, BsonDocument fields, PositionalContext ctx)
    {
        foreach (var element in fields)
        {
            if (HasPositionalOperator(element.Name))
            {
                ApplyPositionalAction(doc, element.Name, ctx, (d, leaf) =>
                {
                    if (!d.Contains(leaf))
                    {
                        d[leaf] = element.Value;
                        return;
                    }
                    var current = d[leaf];
                    if (BsonValueComparer.Instance.Compare(element.Value, current) < 0)
                        d[leaf] = element.Value;
                });
            }
            else
            {
                // Ref: https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/
                //   "MinKey < Null < Numbers" — so $min with a number on a null field is no-op.
                //   But if the field is missing, always set the value.
                var fieldExists = BsonFilterEvaluator.FieldExists(doc, element.Name);
                if (!fieldExists)
                {
                    SetFieldPath(doc, element.Name, element.Value);
                }
                else
                {
                    var current = ResolveFieldPath(doc, element.Name);
                    if (BsonValueComparer.Instance.Compare(element.Value, current) < 0)
                        SetFieldPath(doc, element.Name, element.Value);
                }
            }
        }
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/max/
    //   "Updates the value of the field to the specified value if the specified value
    //    is greater than the current value of the field."
    private static void ApplyMax(BsonDocument doc, BsonDocument fields, PositionalContext ctx)
    {
        foreach (var element in fields)
        {
            if (HasPositionalOperator(element.Name))
            {
                ApplyPositionalAction(doc, element.Name, ctx, (d, leaf) =>
                {
                    if (!d.Contains(leaf))
                    {
                        d[leaf] = element.Value;
                    }
                    else
                    {
                        var current = d[leaf];
                        if (BsonValueComparer.Instance.Compare(element.Value, current) > 0)
                            d[leaf] = element.Value;
                    }
                });
            }
            else
            {
                // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/max/
                //   "If the field does not exist, the $max operator sets the field to the specified value."
                //   If the field exists (even as null), only update if new value is greater.
                var fieldExists = BsonFilterEvaluator.FieldExists(doc, element.Name);
                if (!fieldExists)
                {
                    SetFieldPath(doc, element.Name, element.Value);
                }
                else
                {
                    var current = ResolveFieldPath(doc, element.Name);
                    if (BsonValueComparer.Instance.Compare(element.Value, current) > 0)
                        SetFieldPath(doc, element.Name, element.Value);
                }
            }
        }
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/rename/
    //   "Renames a field."
    //   "$rename does nothing if the field does not exist."
    //   But a field that exists with a null value should be renamed.
    private static void ApplyRename(BsonDocument doc, BsonDocument fields)
    {
        foreach (var element in fields)
        {
            var oldName = element.Name;
            var newName = element.Value.AsString;

            // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/rename/
            //   "The source and target field for $rename must differ."
            if (oldName == newName)
                throw MongoErrors.BadValue(
                    $"The source and target field for $rename must differ: {oldName}: \"{newName}\"");

            // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/rename/
            //   "$rename does not work if these fields are in array elements."
            //   MongoDB errors with "The source field for $rename may not be dynamic"
            if (PathTraversesArray(doc, oldName) || PathTraversesArray(doc, newName))
                throw MongoErrors.RenameTraversesArray(oldName);

            if (BsonFilterEvaluator.FieldExists(doc, oldName))
            {
                var value = ResolveFieldPath(doc, oldName);
                RemoveFieldPath(doc, oldName);
                SetFieldPath(doc, newName, value);
            }
        }
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/rename/
    //   Checks if any intermediate segment of a dotted path is an array element.
    private static bool PathTraversesArray(BsonDocument doc, string path)
    {
        var parts = path.Split('.');
        BsonValue current = doc;
        for (int i = 0; i < parts.Length - 1; i++)
        {
            if (current.IsBsonDocument)
            {
                var d = current.AsBsonDocument;
                if (d.Contains(parts[i]))
                    current = d[parts[i]];
                else if (int.TryParse(parts[i], out _))
                    return true; // numeric index on a document means array traversal intent
                else
                    return false;
            }
            else if (current.IsBsonArray)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        return current.IsBsonArray;
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/currentDate/
    //   "Sets the value of a field to current date, either as a Date or a Timestamp."
    private static void ApplyCurrentDate(BsonDocument doc, BsonDocument fields, PositionalContext ctx)
    {
        foreach (var element in fields)
        {
            BsonValue dateVal;
            if (element.Value.IsBsonDocument)
            {
                var spec = element.Value.AsBsonDocument;
                if (spec.Contains("$type") && spec["$type"].AsString == "timestamp")
                {
                    dateVal = new BsonTimestamp((int)DateTimeOffset.UtcNow.ToUnixTimeSeconds(), 1);
                    SetFieldPathPositional(doc, element.Name, dateVal, ctx);
                    continue;
                }
            }
            dateVal = new BsonDateTime(DateTime.UtcNow);
            SetFieldPathPositional(doc, element.Name, dateVal, ctx);
        }
    }

    #endregion

    #region Array Update Operators

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/push/
    //   "Appends a specified value to an array."
    private static void ApplyPush(BsonDocument doc, BsonDocument fields, PositionalContext ctx)
    {
        foreach (var element in fields)
        {
            var fieldPath = element.Name;

            // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/sort/
            //   Real MongoDB 7.0: If $sort/$slice/$position are used without $each,
            //   the modifier document is pushed as a literal value (no error thrown).
            if (element.Value.IsBsonDocument)
            {
                var pushDoc = element.Value.AsBsonDocument;
                // Only treat as modifiers if $each is present
            }

            if (HasPositionalOperator(fieldPath))
            {
                ApplyPositionalAction(doc, fieldPath, ctx, (d, leaf) =>
                {
                    if (!d.Contains(leaf))
                        d[leaf] = new BsonArray();
                    if (d[leaf] is not BsonArray arr)
                        throw MongoErrors.BadValue($"The field '{fieldPath}' must be an array but is of type {d[leaf].BsonType}");
                    if (element.Value.IsBsonDocument && element.Value.AsBsonDocument.Contains("$each"))
                        ApplyPushEach(arr, element.Value.AsBsonDocument);
                    else
                        arr.Add(element.Value);
                });
                continue;
            }

            var current = ResolveFieldPath(doc, fieldPath);

            // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/push/
            //   If the field is absent, $push creates it as an array.
            //   If the field exists with a null value, it should throw.
            if (current == BsonNull.Value)
            {
                if (BsonFilterEvaluator.FieldExists(doc, fieldPath))
                    throw MongoErrors.BadValue($"The field '{fieldPath}' must be an array but is of type Null");
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
        var position = spec.Contains("$position") ? spec["$position"].ToInt32() : int.MinValue;

        if (position != int.MinValue)
        {
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/position/
            //   "A negative value ... calculates the position relative to the end of the array."
            int insertAt = position >= 0
                ? Math.Min(position, array.Count)
                : Math.Max(0, array.Count + position);
            for (int i = 0; i < items.Count; i++)
                array.Insert(Math.Min(insertAt + i, array.Count), items[i]);
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
                // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/push/
                //   Non-document elements are treated as having missing sort fields.
                var items2 = array.Select(x => x.IsBsonDocument ? x.AsBsonDocument : new BsonDocument()).ToList();
                var indexMap = Enumerable.Range(0, array.Count).ToList();
                var paired = items2.Zip(indexMap, (doc, idx) => (doc, idx)).ToList();
                var sortedPaired = BsonSortEvaluator.Apply(paired.Select(p => p.doc).ToList(), sortDoc);
                // Rebuild using original values preserving non-doc elements
                var sortedIndices = sortedPaired.Select(d => paired.First(p => ReferenceEquals(p.doc, d)).idx).ToList();
                sorted = new BsonArray(sortedIndices.Select(i => array[i]));
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
    private static void ApplyPull(BsonDocument doc, BsonDocument fields, PositionalContext ctx)
    {
        foreach (var element in fields)
        {
            if (HasPositionalOperator(element.Name))
            {
                var condition = element.Value;
                ApplyPositionalAction(doc, element.Name, ctx, (d, leaf) =>
                {
                    if (d[leaf] is not BsonArray arr) return;
                    var toRemove = new List<int>();
                    for (int i = 0; i < arr.Count; i++)
                    {
                        if (ShouldPull(arr[i], condition))
                            toRemove.Add(i);
                    }
                    for (int i = toRemove.Count - 1; i >= 0; i--)
                        arr.RemoveAt(toRemove[i]);
                });
                continue;
            }

            var current = ResolveFieldPath(doc, element.Name);
            if (current == BsonNull.Value) continue; // missing field is a no-op
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/pull/
            //   "If the specified <field> is not an array, the operation will fail."
            if (current is not BsonArray array)
                throw MongoErrors.BadValue($"Cannot apply $pull to a non-array value");

            var cond = element.Value;
            var toRem = new List<int>();
            for (int i = 0; i < array.Count; i++)
            {
                if (ShouldPull(array[i], cond))
                    toRem.Add(i);
            }

            for (int i = toRem.Count - 1; i >= 0; i--)
                array.RemoveAt(toRem[i]);
        }
    }

    private static bool ShouldPull(BsonValue element, BsonValue condition)
    {
        if (condition.IsBsonDocument)
        {
            var condDoc = condition.AsBsonDocument;
            if (condDoc.Names.Any(n => n.StartsWith("$")))
            {
                // Condition is a query expression with operators
                if (element.IsBsonDocument)
                    return BsonFilterEvaluator.Matches(element.AsBsonDocument, condDoc);
                // Apply condition as scalar comparison
                var wrapDoc = new BsonDocument("_v", element);
                var wrapFilter = new BsonDocument("_v", condition);
                return BsonFilterEvaluator.Matches(wrapDoc, wrapFilter);
            }
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/pull/
            //   "To specify a <condition>, use the query filters."
            //   A condition like { field: value } without $ operators matches subdocuments
            //   where field equals value, not exact document equality.
            if (element.IsBsonDocument)
                return BsonFilterEvaluator.Matches(element.AsBsonDocument, condDoc);
            return element.Equals(condition);
        }
        return element.Equals(condition);
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/pullAll/
    //   "Removes all instances of the specified values from an existing array."
    private static void ApplyPullAll(BsonDocument doc, BsonDocument fields, PositionalContext ctx)
    {
        foreach (var element in fields)
        {
            if (HasPositionalOperator(element.Name))
            {
                var valuesToRemove = element.Value.AsBsonArray;
                ApplyPositionalAction(doc, element.Name, ctx, (d, leaf) =>
                {
                    if (d[leaf] is not BsonArray arr) return;
                    for (int i = arr.Count - 1; i >= 0; i--)
                    {
                        if (valuesToRemove.Any(v => v.Equals(arr[i])))
                            arr.RemoveAt(i);
                    }
                });
                continue;
            }

            var current = ResolveFieldPath(doc, element.Name);
            if (current == BsonNull.Value) continue; // missing field is a no-op
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/pullAll/
            //   "If a <field> is not an array, the operation will fail."
            if (current is not BsonArray array)
                throw MongoErrors.BadValue($"Cannot apply $pullAll to a non-array value");

            var vals = element.Value.AsBsonArray;
            for (int i = array.Count - 1; i >= 0; i--)
            {
                if (vals.Any(v => v.Equals(array[i])))
                    array.RemoveAt(i);
            }
        }
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/addToSet/
    //   "Adds a value to an array unless the value is already present."
    private static void ApplyAddToSet(BsonDocument doc, BsonDocument fields, PositionalContext ctx)
    {
        foreach (var element in fields)
        {
            if (HasPositionalOperator(element.Name))
            {
                ApplyPositionalAction(doc, element.Name, ctx, (d, leaf) =>
                {
                    if (!d.Contains(leaf))
                        d[leaf] = new BsonArray();
                    if (d[leaf] is not BsonArray arr)
                        throw MongoErrors.BadValue($"The field '{element.Name}' must be an array");
                    if (element.Value.IsBsonDocument && element.Value.AsBsonDocument.Contains("$each"))
                    {
                        var items = element.Value.AsBsonDocument["$each"].AsBsonArray;
                        foreach (var item in items)
                        {
                            if (!arr.Any(x => x.Equals(item)))
                                arr.Add(item);
                        }
                    }
                    else
                    {
                        if (!arr.Any(x => x.Equals(element.Value)))
                            arr.Add(element.Value);
                    }
                });
                continue;
            }

            var current = ResolveFieldPath(doc, element.Name);
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/addToSet/
            //   If the field is absent, $addToSet creates it as an array.
            //   If the field exists with a null value, it should throw.
            if (current == BsonNull.Value)
            {
                if (BsonFilterEvaluator.FieldExists(doc, element.Name))
                    throw MongoErrors.BadValue($"The field '{element.Name}' must be an array but is of type Null");
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
    private static void ApplyPop(BsonDocument doc, BsonDocument fields, PositionalContext ctx)
    {
        foreach (var element in fields)
        {
            if (HasPositionalOperator(element.Name))
            {
                var direction = element.Value.ToInt32();
                ApplyPositionalAction(doc, element.Name, ctx, (d, leaf) =>
                {
                    if (d[leaf] is not BsonArray arr || arr.Count == 0) return;
                    if (direction == -1)
                        arr.RemoveAt(0);
                    else
                        arr.RemoveAt(arr.Count - 1);
                });
                continue;
            }

            var current = ResolveFieldPath(doc, element.Name);
            if (current == BsonNull.Value) continue; // missing field is a no-op
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/pop/
            //   "If <field> is not an array, $pop fails."
            if (current is not BsonArray array)
                throw MongoErrors.BadValue($"Path '{element.Name}' contains an element of non-array type '{current.BsonType}'");
            if (array.Count == 0) continue;

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
    private static void ApplyBit(BsonDocument doc, BsonDocument fields, PositionalContext ctx)
    {
        foreach (var element in fields)
        {
            if (HasPositionalOperator(element.Name))
            {
                var ops = element.Value.AsBsonDocument;
                ApplyPositionalAction(doc, element.Name, ctx, (d, leaf) =>
                {
                    var cur = d.Contains(leaf) ? d[leaf] : new BsonInt32(0);
                    long curVal = cur.IsInt64 ? cur.AsInt64 : cur.IsInt32 ? cur.AsInt32 : 0;
                    foreach (var op in ops)
                    {
                        long operand = op.Value.IsInt64 ? op.Value.AsInt64 : op.Value.AsInt32;
                        curVal = op.Name switch
                        {
                            "and" => curVal & operand,
                            "or" => curVal | operand,
                            "xor" => curVal ^ operand,
                            _ => throw MongoErrors.BadValue($"Unknown bit operation: {op.Name}")
                        };
                    }
                    d[leaf] = cur.IsInt64 ? new BsonInt64(curVal) : new BsonInt32((int)curVal);
                });
                continue;
            }

            var current = ResolveFieldPath(doc, element.Name);
            // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/bit/
            //   "$bit requires the field to hold a numeric value (Int32 or Int64)."
            if (current != BsonNull.Value && !current.IsInt32 && !current.IsInt64)
                throw MongoErrors.BadValue($"Cannot apply $bit to a value of non-numeric type {current.BsonType}");
            long currentVal = current.IsInt64 ? current.AsInt64 : current.IsInt32 ? current.AsInt32 : 0;
            bool isMissing = current == BsonNull.Value;

            var bitOps = element.Value.AsBsonDocument;
            bool anyOperandIsLong = false;
            foreach (var op in bitOps)
            {
                if (op.Value.IsInt64) anyOperandIsLong = true;
                long operand = op.Value.IsInt64 ? op.Value.AsInt64 : op.Value.AsInt32;
                currentVal = op.Name switch
                {
                    "and" => currentVal & operand,
                    "or" => currentVal | operand,
                    "xor" => currentVal ^ operand,
                    _ => throw MongoErrors.BadValue($"Unknown bit operation: {op.Name}")
                };
            }

            // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/bit/
            //   Result type: if field exists, preserve its type; if missing, match the operand type.
            if (current.IsInt64 || (isMissing && anyOperandIsLong))
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
    /// Handles numeric indices for array element access.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/operator/update/set/#set-elements-in-arrays
    ///   "To specify a field in an embedded document or in an array, use dot notation."
    /// </remarks>
    internal static void SetFieldPath(BsonDocument doc, string path, BsonValue value)
    {
        if (!path.Contains('.'))
        {
            doc[path] = value;
            return;
        }

        var parts = path.Split('.');
        BsonValue current = doc;

        for (int i = 0; i < parts.Length - 1; i++)
        {
            if (current is BsonArray arr && int.TryParse(parts[i], out var idx))
            {
                if (idx >= 0 && idx < arr.Count)
                    current = arr[idx];
                else
                    return; // Index out of bounds — no-op
            }
            else if (current is BsonDocument curDoc)
            {
                if (!curDoc.Contains(parts[i]))
                {
                    curDoc[parts[i]] = new BsonDocument();
                }
                else if (!curDoc[parts[i]].IsBsonDocument && !curDoc[parts[i]].IsBsonArray)
                {
                    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/set/
                    //   MongoDB throws PathNotViable (code 28) when a dotted path traverses a scalar.
                    throw MongoErrors.BadValue(
                        $"Cannot create field '{parts[i + 1]}' in element {{{parts[i]}: {curDoc[parts[i]]}}}");
                }
                current = curDoc[parts[i]];
            }
            else
            {
                return; // Can't navigate further
            }
        }

        var leaf = parts[^1];
        if (current is BsonArray leafArr && int.TryParse(leaf, out var leafIdx))
        {
            if (leafIdx >= 0 && leafIdx < leafArr.Count)
                leafArr[leafIdx] = value;
        }
        else if (current is BsonDocument leafDoc)
        {
            leafDoc[leaf] = value;
        }
    }

    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/operator/update/unset/
    ///   "If the field does not exist, then $unset does nothing."
    ///   When unsetting an array element by index, MongoDB sets it to null.
    /// </remarks>
    private static void RemoveFieldPath(BsonDocument doc, string path)
    {
        if (!path.Contains('.'))
        {
            doc.Remove(path);
            return;
        }

        var parts = path.Split('.');
        BsonValue current = doc;

        for (int i = 0; i < parts.Length - 1; i++)
        {
            if (current is BsonArray arr && int.TryParse(parts[i], out var idx))
            {
                if (idx >= 0 && idx < arr.Count)
                    current = arr[idx];
                else
                    return;
            }
            else if (current is BsonDocument curDoc)
            {
                if (!curDoc.Contains(parts[i])) return;
                current = curDoc[parts[i]];
                if (!current.IsBsonDocument && !current.IsBsonArray) return;
            }
            else
            {
                return;
            }
        }

        var leaf = parts[^1];
        if (current is BsonArray leafArr && int.TryParse(leaf, out var leafIdx))
        {
            // MongoDB sets array elements to null when unsetting by index
            if (leafIdx >= 0 && leafIdx < leafArr.Count)
                leafArr[leafIdx] = BsonNull.Value;
        }
        else if (current is BsonDocument leafDoc)
        {
            leafDoc.Remove(leaf);
        }
    }

    private static BsonValue AddBsonValues(BsonValue a, BsonValue b)
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/inc/
        //   "Cannot apply $inc to a value of non-numeric type."
        if (a == BsonNull.Value)
            throw MongoErrors.BadValue($"Cannot apply $inc to a value of non-numeric type null");

        if (!a.IsNumeric)
            throw MongoErrors.BadValue($"Cannot apply $inc to a value of non-numeric type {a.BsonType}");

        // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/inc/
        //   Type precedence: Decimal128 > Double > Int64 > Int32
        return (a.BsonType, b.BsonType) switch
        {
            (BsonType.Decimal128, _) or (_, BsonType.Decimal128) =>
                new BsonDecimal128((Decimal128)(ToDecimalSafe(a) + ToDecimalSafe(b))),
            (BsonType.Double, _) or (_, BsonType.Double) =>
                new BsonDouble(a.ToDouble() + b.ToDouble()),
            (BsonType.Int64, _) or (_, BsonType.Int64) =>
                new BsonInt64(a.ToInt64() + b.ToInt64()),
            _ =>
                new BsonInt32(a.ToInt32() + b.ToInt32()),
        };
    }

    private static BsonValue MultiplyBsonValues(BsonValue a, BsonValue b)
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/mul/
        //   "Cannot apply $mul to a value of non-numeric type."
        if (!a.IsNumeric)
            throw MongoErrors.BadValue($"Cannot apply $mul to a value of non-numeric type {a.BsonType}");

        // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/mul/
        //   Type precedence: Decimal128 > Double > Int64 > Int32
        return (a.BsonType, b.BsonType) switch
        {
            (BsonType.Decimal128, _) or (_, BsonType.Decimal128) =>
                new BsonDecimal128((Decimal128)(ToDecimalSafe(a) * ToDecimalSafe(b))),
            (BsonType.Double, _) or (_, BsonType.Double) =>
                new BsonDouble(a.ToDouble() * b.ToDouble()),
            (BsonType.Int64, _) or (_, BsonType.Int64) =>
                new BsonInt64(a.ToInt64() * b.ToInt64()),
            _ =>
                new BsonInt32(a.ToInt32() * b.ToInt32()),
        };
    }

    private static decimal ToDecimalSafe(BsonValue v) => v.BsonType switch
    {
        BsonType.Decimal128 => Decimal128.ToDecimal(v.AsDecimal128),
        BsonType.Double => (decimal)v.AsDouble,
        BsonType.Int64 => v.AsInt64,
        _ => v.ToInt32(),
    };

    #endregion

    #region Positional Operators

    /// <summary>
    /// Context for positional update operators ($, $[], $[identifier]).
    /// </summary>
    private record PositionalContext(
        int MatchedArrayIndex,
        string? MatchedArrayField,
        IReadOnlyList<BsonDocument>? ArrayFilters);

    /// <summary>
    /// Returns true if the path contains a positional operator ($, $[], or $[identifier]).
    /// </summary>
    private static bool HasPositionalOperator(string path)
    {
        var parts = path.Split('.');
        return parts.Any(p => p == "$" || p == "$[]" || (p.StartsWith("$[") && p.EndsWith("]")));
    }

    /// <summary>
    /// Sets a value at a path that may contain positional operators.
    /// Delegates to SetFieldPath if no positional operators are present.
    /// </summary>
    /// <remarks>
    /// Ref: https://www.mongodb.com/docs/manual/reference/operator/update/positional/
    ///   "The positional $ operator identifies an element in an array."
    /// Ref: https://www.mongodb.com/docs/manual/reference/operator/update/positional-all/
    ///   "The all positional operator $[] indicates all elements in the array."
    /// Ref: https://www.mongodb.com/docs/manual/reference/operator/update/positional-filtered/
    ///   "The filtered positional operator $[<identifier>] identifies array elements
    ///    that match the arrayFilters conditions."
    /// </remarks>
    private static void SetFieldPathPositional(BsonDocument doc, string path, BsonValue value, PositionalContext ctx)
    {
        if (!HasPositionalOperator(path))
        {
            SetFieldPath(doc, path, value);
            return;
        }

        ApplyPositionalAction(doc, path, ctx, (d, leaf) => d[leaf] = value);
    }

    /// <summary>
    /// Applies an action to elements targeted by positional operators in the path.
    /// </summary>
    private static void ApplyPositionalAction(BsonDocument doc, string path, PositionalContext ctx,
        Action<BsonDocument, string> action)
    {
        var parts = path.Split('.');
        ApplyPositionalRecursive(doc, parts, 0, ctx, action);
    }

    private static void ApplyPositionalRecursive(BsonDocument doc, string[] parts, int partIndex,
        PositionalContext ctx, Action<BsonDocument, string> action)
    {
        if (partIndex >= parts.Length) return;

        var part = parts[partIndex];
        bool isLast = partIndex == parts.Length - 1;

        if (part == "$")
        {
            // $ positional: use the matched array index
            // The previous part is the array field
            // We should already be inside the array element at this point
            // $ is resolved by walking to the matched index of the parent array
            if (partIndex == 0)
            {
                // $ at top level is the matched array field itself — shouldn't happen
                return;
            }
            // We're already positioned by the caller. In the recursive model,
            // $ means we already walked into the array. The parent calls this
            // for only the matched index.
            // This case is handled by the parent array iteration.
            return;
        }

        if (part == "$[]")
        {
            // $[] all positional — shouldn't appear at top level; handled by parent
            return;
        }

        if (part.StartsWith("$[") && part.EndsWith("]"))
        {
            // $[identifier] filtered positional — handled by parent
            return;
        }

        // Regular field name
        if (!doc.Contains(part)) return;
        var fieldVal = doc[part];

        // Check if the NEXT part is a positional operator
        if (partIndex + 1 < parts.Length)
        {
            var nextPart = parts[partIndex + 1];
            var remainingParts = parts.Skip(partIndex + 2).ToArray();

            if (nextPart == "$")
            {
                // $ positional: operate on the matched index only
                if (fieldVal is BsonArray arr && ctx.MatchedArrayIndex >= 0 && ctx.MatchedArrayIndex < arr.Count)
                {
                    if (remainingParts.Length == 0)
                    {
                        // e.g., "scores.$" → operate on arr[matchedIndex]
                        var wrapper = CreateArrayIndexWrapper(arr, ctx.MatchedArrayIndex);
                        action(wrapper, "_v");
                        arr[ctx.MatchedArrayIndex] = wrapper["_v"];
                    }
                    else
                    {
                        // e.g., "items.$.qty" → navigate into arr[matchedIndex].qty
                        if (arr[ctx.MatchedArrayIndex] is BsonDocument subDoc)
                        {
                            ApplyPositionalRecursive(subDoc, remainingParts, 0, ctx, action);
                        }
                    }
                }
                return;
            }

            if (nextPart == "$[]")
            {
                // $[] all positional: operate on all elements
                if (fieldVal is BsonArray arr)
                {
                    for (int i = 0; i < arr.Count; i++)
                    {
                        if (remainingParts.Length == 0)
                        {
                            var wrapper = CreateArrayIndexWrapper(arr, i);
                            action(wrapper, "_v");
                            arr[i] = wrapper["_v"];
                        }
                        else if (arr[i] is BsonDocument subDoc)
                        {
                            ApplyPositionalRecursive(subDoc, remainingParts, 0, ctx, action);
                        }
                    }
                }
                return;
            }

            if (nextPart.StartsWith("$[") && nextPart.EndsWith("]"))
            {
                // $[identifier] filtered positional
                var identifier = nextPart[2..^1]; // extract identifier name
                if (fieldVal is BsonArray arr && ctx.ArrayFilters != null)
                {
                    // Find the matching filter for this identifier
                    var filter = FindArrayFilter(ctx.ArrayFilters, identifier);
                    if (filter != null)
                    {
                        for (int i = 0; i < arr.Count; i++)
                        {
                            if (MatchesArrayFilter(arr[i], filter, identifier))
                            {
                                if (remainingParts.Length == 0)
                                {
                                    var wrapper = CreateArrayIndexWrapper(arr, i);
                                    action(wrapper, "_v");
                                    arr[i] = wrapper["_v"];
                                }
                                else if (arr[i] is BsonDocument subDoc)
                                {
                                    ApplyPositionalRecursive(subDoc, remainingParts, 0, ctx, action);
                                }
                            }
                        }
                    }
                }
                return;
            }
        }

        // Regular navigation: go deeper
        if (isLast)
        {
            action(doc, part);
        }
        else if (fieldVal is BsonDocument subDoc2)
        {
            ApplyPositionalRecursive(subDoc2, parts, partIndex + 1, ctx, action);
        }
    }

    /// <summary>Creates a wrapper doc to use as an action target for array element updates.</summary>
    private static BsonDocument CreateArrayIndexWrapper(BsonArray arr, int index)
    {
        return new BsonDocument("_v", arr[index]);
    }

    /// <summary>Finds the array filter definition for a given identifier.</summary>
    private static BsonDocument? FindArrayFilter(IReadOnlyList<BsonDocument> filters, string identifier)
    {
        // Array filters use the identifier as a prefix in field names
        // e.g., identifier "elem" matches filter { "elem": { "$lt": 50 } } or { "elem.status": "pending" }
        return filters.FirstOrDefault(f =>
            f.Names.Any(n => n == identifier || n.StartsWith(identifier + ".")));
    }

    /// <summary>Tests whether an array element matches the given array filter.</summary>
    private static bool MatchesArrayFilter(BsonValue element, BsonDocument filter, string identifier)
    {
        // Rewrite filter to replace identifier prefix with actual field paths
        // For scalar elements: { "elem": { "$lt": 50 } } → test element < 50
        // For subdoc elements: { "elem.status": "pending" } → test element.status == "pending"
        var rewrittenFilter = new BsonDocument();
        foreach (var el in filter)
        {
            if (el.Name == identifier)
            {
                // Direct scalar comparison: wrap element in a doc
                rewrittenFilter["_v"] = el.Value;
            }
            else if (el.Name.StartsWith(identifier + "."))
            {
                // Subdocument field: strip the identifier prefix
                var subPath = el.Name.Substring(identifier.Length + 1);
                rewrittenFilter[subPath] = el.Value;
            }
        }

        if (element is BsonDocument doc)
        {
            return BsonFilterEvaluator.Matches(doc, rewrittenFilter);
        }

        // Scalar element: wrap in { "_v": element } and match against rewritten filter
        var wrapper = new BsonDocument("_v", element);
        return BsonFilterEvaluator.Matches(wrapper, rewrittenFilter);
    }

    /// <summary>
    /// Determines the matched array index for the $ positional operator by finding
    /// which array element was matched by the query filter.
    /// </summary>
    internal static (int index, string? field) FindMatchedArrayIndex(BsonDocument document, BsonDocument filter)
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/positional/
        //   "The positional $ operator acts as a placeholder for the first element
        //    that matches the query document."
        foreach (var el in filter)
        {
            if (el.Name.StartsWith("$")) continue; // Skip $and, $or, etc.

            var dotIndex = el.Name.IndexOf('.');
            string arrayField;
            string? subField = null;

            if (dotIndex > 0)
            {
                // "items.name" → arrayField = "items", subField = "name"
                arrayField = el.Name.Substring(0, dotIndex);
                subField = el.Name.Substring(dotIndex + 1);
            }
            else
            {
                arrayField = el.Name;
            }

            if (!document.Contains(arrayField)) continue;
            var fieldVal = document[arrayField];
            if (fieldVal is not BsonArray arr) continue;

            // Find the first matching element
            for (int i = 0; i < arr.Count; i++)
            {
                bool matches;
                if (subField != null && arr[i] is BsonDocument subDoc)
                {
                    // Match against subdocument field
                    var subFilter = new BsonDocument(subField, el.Value);
                    matches = BsonFilterEvaluator.Matches(subDoc, subFilter);
                }
                else
                {
                    // Direct value match or query operator match on scalar array element
                    // Ref: https://www.mongodb.com/docs/manual/reference/operator/update/positional/
                    //   "The positional $ operator acts as a placeholder for the first element
                    //    that matches the query document."
                    if (el.Value.IsBsonDocument && el.Value.AsBsonDocument.Names.Any(n => n.StartsWith("$")))
                    {
                        // Query operator condition (e.g., { scores: { $gte: 8 } })
                        var wrapperDoc = new BsonDocument(el.Name, arr[i]);
                        var filterDoc = new BsonDocument(el.Name, el.Value);
                        matches = BsonFilterEvaluator.Matches(wrapperDoc, filterDoc);
                    }
                    else
                    {
                        matches = arr[i].Equals(el.Value);
                    }
                }

                if (matches)
                    return (i, arrayField);
            }
        }

        return (-1, null);
    }

    #endregion
}
