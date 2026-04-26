using MongoDB.Bson;

namespace MongoDB.InMemoryEmulator;

/// <summary>
/// Executes MongoDB aggregation pipelines — the core query engine.
/// Each pipeline stage transforms the document stream sequentially.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation-pipeline/
///   "An aggregation pipeline consists of one or more stages that process documents."
/// </remarks>
internal static class AggregationPipelineExecutor
{
    /// <summary>
    /// Execute an aggregation pipeline against an input document stream.
    /// </summary>
    internal static IEnumerable<BsonDocument> Execute(
        IEnumerable<BsonDocument> input,
        IReadOnlyList<BsonDocument> stages,
        AggregationContext context)
    {
        IEnumerable<BsonDocument> current = input;

        foreach (var stage in stages)
        {
            var stageName = stage.Names.First();
            var stageSpec = stage[stageName];

            // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation-pipeline/
            current = stageName switch
            {
                "$match" => ExecuteMatch(current, stageSpec.AsBsonDocument, context),
                "$project" => ExecuteProject(current, stageSpec.AsBsonDocument),
                "$addFields" or "$set" => ExecuteAddFields(current, stageSpec.AsBsonDocument),
                "$unset" => ExecuteUnset(current, stageSpec),
                "$group" => ExecuteGroup(current, stageSpec.AsBsonDocument),
                "$sort" => ExecuteSort(current, stageSpec.AsBsonDocument),
                "$limit" => current.Take(stageSpec.ToInt32()),
                "$skip" => current.Skip(stageSpec.ToInt32()),
                "$unwind" => ExecuteUnwind(current, stageSpec),
                "$lookup" => ExecuteLookup(current, stageSpec.AsBsonDocument, context),
                "$replaceRoot" => ExecuteReplaceRoot(current, stageSpec.AsBsonDocument),
                "$replaceWith" => ExecuteReplaceWith(current, stageSpec),
                "$count" => ExecuteCount(current, stageSpec.AsString),
                "$sortByCount" => ExecuteSortByCount(current, stageSpec),
                "$sample" => ExecuteSample(current, stageSpec.AsBsonDocument),
                "$facet" => ExecuteFacet(current, stageSpec.AsBsonDocument, context),
                "$bucket" => ExecuteBucket(current, stageSpec.AsBsonDocument),
                "$bucketAuto" => ExecuteBucketAuto(current, stageSpec.AsBsonDocument),
                "$unionWith" => ExecuteUnionWith(current, stageSpec.AsBsonDocument, context),
                "$graphLookup" => ExecuteGraphLookup(current, stageSpec.AsBsonDocument, context),
                "$redact" => ExecuteRedact(current, stageSpec),
                "$merge" => ExecuteMerge(current, stageSpec.AsBsonDocument, context),
                "$out" => ExecuteOut(current, stageSpec, context),
                "$setWindowFields" => ExecuteSetWindowFields(current, stageSpec.AsBsonDocument),
                "$densify" => ExecuteDensify(current, stageSpec.AsBsonDocument),
                "$fill" => ExecuteFill(current, stageSpec.AsBsonDocument),
                "$documents" => ExecuteDocuments(stageSpec.AsBsonArray),
                "$collStats" => ExecuteCollStats(stageSpec.AsBsonDocument, context),
                "$indexStats" => ExecuteIndexStats(context),
                // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/geoNear/
                //   "Returns documents ordered by proximity to a specified point."
                "$geoNear" => ExecuteGeoNear(current, stageSpec.AsBsonDocument),
                _ => throw new NotSupportedException($"Aggregation stage '{stageName}' is not supported.")
            };
        }

        return current;
    }

    #region Core Stages

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/match/
    //   "Filters the documents to pass only the documents that match the specified condition(s)."
    private static IEnumerable<BsonDocument> ExecuteMatch(IEnumerable<BsonDocument> input, BsonDocument filter, AggregationContext context)
    {
        var variables = context.Variables;
        return input.Where(doc => BsonFilterEvaluator.Matches(doc, filter, variables));
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/project/
    //   "Passes along the documents with the requested fields to the next stage in the pipeline."
    private static IEnumerable<BsonDocument> ExecuteProject(IEnumerable<BsonDocument> input, BsonDocument spec)
    {
        return input.Select(doc => ApplyProjection(doc, spec));
    }

    internal static BsonDocument ApplyProjection(BsonDocument doc, BsonDocument spec)
    {
        // Determine mode: inclusion vs exclusion vs expression-only
        bool hasIncludes = false;
        bool hasExpressions = false;

        foreach (var el in spec)
        {
            if (el.Name == "_id") continue;
            if (el.Value.IsBsonDocument)
            {
                var innerDoc = el.Value.AsBsonDocument;
                if (innerDoc.ElementCount > 0 && innerDoc.Names.First().StartsWith("$"))
                    hasExpressions = true;
                else
                    hasIncludes = true; // nested inclusion like { "a.b": 1 }
            }
            else if (el.Value.IsNumeric || el.Value.IsBoolean)
            {
                if (el.Value.ToBoolean()) hasIncludes = true;
            }
            else
            {
                hasExpressions = true; // string field expression like "$fieldName"
            }
        }

        // Expression-only or mixed with includes → inclusion mode
        // Exclusion mode: all non-_id specs evaluate to false
        bool inclusionMode = hasIncludes || hasExpressions;

        var result = new BsonDocument();

        // Handle _id
        if (spec.Contains("_id"))
        {
            var idSpec = spec["_id"];
            if (idSpec.IsBsonDocument || (idSpec is BsonString s && s.Value.StartsWith("$")))
            {
                result["_id"] = AggregationExpressionEvaluator.Evaluate(doc, idSpec);
            }
            else if (idSpec.ToBoolean())
            {
                if (doc.Contains("_id")) result["_id"] = doc["_id"];
            }
            // else: _id:0 → exclude
        }
        else if (inclusionMode)
        {
            // Default: include _id in inclusion mode
            if (doc.Contains("_id")) result["_id"] = doc["_id"];
        }

        if (inclusionMode)
        {
            foreach (var el in spec)
            {
                if (el.Name == "_id") continue;
                if (el.Value.IsBsonDocument)
                {
                    var innerDoc = el.Value.AsBsonDocument;
                    if (innerDoc.ElementCount > 0 && innerDoc.Names.First().StartsWith("$"))
                    {
                        // Expression
                        result[el.Name] = AggregationExpressionEvaluator.Evaluate(doc, el.Value);
                    }
                    else
                    {
                        // Simple numeric inclusion
                        if (el.Value.ToBoolean())
                        {
                            var val = BsonFilterEvaluator.ResolveFieldPath(doc, el.Name);
                            if (val != BsonNull.Value) result[el.Name] = val;
                        }
                    }
                }
                else if (el.Value is BsonString strVal && strVal.Value.StartsWith("$"))
                {
                    // Field expression like "$fieldName"
                    result[el.Name] = AggregationExpressionEvaluator.Evaluate(doc, el.Value);
                }
                else if (el.Value.ToBoolean())
                {
                    var val = BsonFilterEvaluator.ResolveFieldPath(doc, el.Name);
                    if (val != BsonNull.Value) result[el.Name] = val;
                }
            }
        }
        else
        {
            // Exclusion mode: copy all fields except excluded ones
            var excluded = new HashSet<string>(spec.Names.Where(n => !spec[n].ToBoolean()));
            foreach (var el in doc)
            {
                if (!excluded.Contains(el.Name))
                    result[el.Name] = el.Value;
            }
        }

        return result;
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/addFields/
    //   "Adds new fields to documents. $addFields outputs documents that contain all existing fields
    //    from the input documents and newly added fields."
    private static IEnumerable<BsonDocument> ExecuteAddFields(IEnumerable<BsonDocument> input, BsonDocument spec)
    {
        return input.Select(doc =>
        {
            var clone = doc.DeepClone().AsBsonDocument;
            foreach (var el in spec)
            {
                var val = AggregationExpressionEvaluator.Evaluate(doc, el.Value);
                SetFieldPath(clone, el.Name, val);
            }
            return clone;
        });
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/unset/
    //   "Removes/excludes fields from documents."
    private static IEnumerable<BsonDocument> ExecuteUnset(IEnumerable<BsonDocument> input, BsonValue spec)
    {
        var fields = spec is BsonArray arr
            ? arr.Select(v => v.AsString).ToList()
            : new List<string> { spec.AsString };

        return input.Select(doc =>
        {
            var clone = doc.DeepClone().AsBsonDocument;
            foreach (var field in fields)
                clone.Remove(field);
            return clone;
        });
    }

    #endregion

    #region $group

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/group/
    //   "Groups input documents by the specified _id expression and for each distinct grouping,
    //    outputs a document."
    private static IEnumerable<BsonDocument> ExecuteGroup(IEnumerable<BsonDocument> input, BsonDocument spec)
    {
        var idExpr = spec["_id"];
        var accumulators = spec.Elements.Where(e => e.Name != "_id").ToList();

        var docs = input.ToList();
        var groups = docs.GroupBy(
            doc => AggregationExpressionEvaluator.Evaluate(doc, idExpr),
            BsonValueComparer.Instance);

        foreach (var group in groups)
        {
            var result = new BsonDocument { { "_id", group.Key } };
            var groupDocs = group.ToList();

            foreach (var acc in accumulators)
            {
                var accSpec = acc.Value.AsBsonDocument;
                var op = accSpec.Names.First();
                var fieldExpr = accSpec[op];

                result[acc.Name] = op switch
                {
                    "$sum" => ComputeSum(groupDocs, fieldExpr),
                    "$avg" => ComputeAvg(groupDocs, fieldExpr),
                    "$min" => ComputeMin(groupDocs, fieldExpr),
                    "$max" => ComputeMax(groupDocs, fieldExpr),
                    "$first" => ComputeFirst(groupDocs, fieldExpr),
                    "$last" => ComputeLast(groupDocs, fieldExpr),
                    "$push" => new BsonArray(groupDocs.Select(d => AggregationExpressionEvaluator.Evaluate(d, fieldExpr))),
                    "$addToSet" => new BsonArray(groupDocs.Select(d => AggregationExpressionEvaluator.Evaluate(d, fieldExpr)).Distinct(BsonValueComparer.Instance)),
                    "$count" => new BsonInt32(groupDocs.Count),
                    "$mergeObjects" => ComputeMergeObjects(groupDocs, fieldExpr),
                    "$stdDevPop" => ComputeStdDevPop(groupDocs, fieldExpr),
                    "$stdDevSamp" => ComputeStdDevSamp(groupDocs, fieldExpr),
                    "$top" => ComputeTop(groupDocs, accSpec),
                    "$bottom" => ComputeBottom(groupDocs, accSpec),
                    "$topN" => ComputeTopN(groupDocs, accSpec),
                    "$bottomN" => ComputeBottomN(groupDocs, accSpec),
                    "$firstN" => ComputeFirstN(groupDocs, accSpec),
                    "$lastN" => ComputeLastN(groupDocs, accSpec),
                    "$maxN" => ComputeMaxN(groupDocs, accSpec),
                    "$minN" => ComputeMinN(groupDocs, accSpec),
                    _ => throw new NotSupportedException($"Accumulator '{op}' is not supported.")
                };
            }

            yield return result;
        }
    }

    private static BsonValue ComputeSum(List<BsonDocument> docs, BsonValue fieldExpr)
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/sum/
        if (fieldExpr is BsonInt32 literal)
            return new BsonInt64((long)literal.Value * docs.Count);

        double sum = 0;
        foreach (var doc in docs)
        {
            var val = AggregationExpressionEvaluator.Evaluate(doc, fieldExpr);
            if (val.IsNumeric) sum += val.ToDouble();
        }
        return new BsonDouble(sum);
    }

    private static BsonValue ComputeAvg(List<BsonDocument> docs, BsonValue fieldExpr)
    {
        double sum = 0;
        int count = 0;
        foreach (var doc in docs)
        {
            var val = AggregationExpressionEvaluator.Evaluate(doc, fieldExpr);
            if (val.IsNumeric) { sum += val.ToDouble(); count++; }
        }
        return count == 0 ? BsonNull.Value : new BsonDouble(sum / count);
    }

    private static BsonValue ComputeMin(List<BsonDocument> docs, BsonValue fieldExpr)
    {
        BsonValue min = BsonNull.Value;
        foreach (var doc in docs)
        {
            var val = AggregationExpressionEvaluator.Evaluate(doc, fieldExpr);
            if (val == BsonNull.Value) continue;
            if (min == BsonNull.Value || BsonValueComparer.Instance.Compare(val, min) < 0)
                min = val;
        }
        return min;
    }

    private static BsonValue ComputeMax(List<BsonDocument> docs, BsonValue fieldExpr)
    {
        BsonValue max = BsonNull.Value;
        foreach (var doc in docs)
        {
            var val = AggregationExpressionEvaluator.Evaluate(doc, fieldExpr);
            if (val == BsonNull.Value) continue;
            if (max == BsonNull.Value || BsonValueComparer.Instance.Compare(val, max) > 0)
                max = val;
        }
        return max;
    }

    private static BsonValue ComputeFirst(List<BsonDocument> docs, BsonValue fieldExpr)
    {
        return docs.Count > 0 ? AggregationExpressionEvaluator.Evaluate(docs[0], fieldExpr) : BsonNull.Value;
    }

    private static BsonValue ComputeLast(List<BsonDocument> docs, BsonValue fieldExpr)
    {
        return docs.Count > 0 ? AggregationExpressionEvaluator.Evaluate(docs[^1], fieldExpr) : BsonNull.Value;
    }

    private static BsonValue ComputeMergeObjects(List<BsonDocument> docs, BsonValue fieldExpr)
    {
        var result = new BsonDocument();
        foreach (var doc in docs)
        {
            var val = AggregationExpressionEvaluator.Evaluate(doc, fieldExpr);
            if (val.IsBsonDocument)
                foreach (var el in val.AsBsonDocument)
                    result[el.Name] = el.Value;
        }
        return result;
    }

    private static BsonValue ComputeStdDevPop(List<BsonDocument> docs, BsonValue fieldExpr)
    {
        var values = docs.Select(d => AggregationExpressionEvaluator.Evaluate(d, fieldExpr))
            .Where(v => v.IsNumeric).Select(v => v.ToDouble()).ToList();
        if (values.Count == 0) return BsonNull.Value;
        double avg = values.Average();
        double variance = values.Sum(v => (v - avg) * (v - avg)) / values.Count;
        return new BsonDouble(Math.Sqrt(variance));
    }

    private static BsonValue ComputeStdDevSamp(List<BsonDocument> docs, BsonValue fieldExpr)
    {
        var values = docs.Select(d => AggregationExpressionEvaluator.Evaluate(d, fieldExpr))
            .Where(v => v.IsNumeric).Select(v => v.ToDouble()).ToList();
        if (values.Count < 2) return BsonNull.Value;
        double avg = values.Average();
        double variance = values.Sum(v => (v - avg) * (v - avg)) / (values.Count - 1);
        return new BsonDouble(Math.Sqrt(variance));
    }

    private static BsonValue ComputeTop(List<BsonDocument> docs, BsonDocument accSpec)
    {
        var topSpec = accSpec["$top"].AsBsonDocument;
        var sortBy = topSpec["sortBy"].AsBsonDocument;
        var output = topSpec["output"];
        var sorted = BsonSortEvaluator.Apply(docs, sortBy);
        return sorted.Count > 0 ? AggregationExpressionEvaluator.Evaluate(sorted[0], output) : BsonNull.Value;
    }

    private static BsonValue ComputeBottom(List<BsonDocument> docs, BsonDocument accSpec)
    {
        var bottomSpec = accSpec["$bottom"].AsBsonDocument;
        var sortBy = bottomSpec["sortBy"].AsBsonDocument;
        var output = bottomSpec["output"];
        var sorted = BsonSortEvaluator.Apply(docs, sortBy);
        return sorted.Count > 0 ? AggregationExpressionEvaluator.Evaluate(sorted[^1], output) : BsonNull.Value;
    }

    private static BsonValue ComputeTopN(List<BsonDocument> docs, BsonDocument accSpec)
    {
        var spec = accSpec["$topN"].AsBsonDocument;
        var sortBy = spec["sortBy"].AsBsonDocument;
        var output = spec["output"];
        var n = spec["n"].ToInt32();
        var sorted = BsonSortEvaluator.Apply(docs, sortBy);
        return new BsonArray(sorted.Take(n).Select(d => AggregationExpressionEvaluator.Evaluate(d, output)));
    }

    private static BsonValue ComputeBottomN(List<BsonDocument> docs, BsonDocument accSpec)
    {
        var spec = accSpec["$bottomN"].AsBsonDocument;
        var sortBy = spec["sortBy"].AsBsonDocument;
        var output = spec["output"];
        var n = spec["n"].ToInt32();
        var sorted = BsonSortEvaluator.Apply(docs, sortBy);
        return new BsonArray(sorted.Skip(Math.Max(0, sorted.Count - n)).Select(d => AggregationExpressionEvaluator.Evaluate(d, output)));
    }

    private static BsonValue ComputeFirstN(List<BsonDocument> docs, BsonDocument accSpec)
    {
        var spec = accSpec["$firstN"].AsBsonDocument;
        var input = spec["input"];
        var n = spec["n"].ToInt32();
        return new BsonArray(docs.Take(n).Select(d => AggregationExpressionEvaluator.Evaluate(d, input)));
    }

    private static BsonValue ComputeLastN(List<BsonDocument> docs, BsonDocument accSpec)
    {
        var spec = accSpec["$lastN"].AsBsonDocument;
        var input = spec["input"];
        var n = spec["n"].ToInt32();
        return new BsonArray(docs.Skip(Math.Max(0, docs.Count - n)).Select(d => AggregationExpressionEvaluator.Evaluate(d, input)));
    }

    private static BsonValue ComputeMaxN(List<BsonDocument> docs, BsonDocument accSpec)
    {
        var spec = accSpec["$maxN"].AsBsonDocument;
        var input = spec["input"];
        var n = spec["n"].ToInt32();
        var values = docs.Select(d => AggregationExpressionEvaluator.Evaluate(d, input))
            .Where(v => v != BsonNull.Value)
            .OrderByDescending(v => v, BsonValueComparer.Instance)
            .Take(n).ToList();
        return new BsonArray(values);
    }

    private static BsonValue ComputeMinN(List<BsonDocument> docs, BsonDocument accSpec)
    {
        var spec = accSpec["$minN"].AsBsonDocument;
        var input = spec["input"];
        var n = spec["n"].ToInt32();
        var values = docs.Select(d => AggregationExpressionEvaluator.Evaluate(d, input))
            .Where(v => v != BsonNull.Value)
            .OrderBy(v => v, BsonValueComparer.Instance)
            .Take(n).ToList();
        return new BsonArray(values);
    }

    #endregion

    #region Sort / Limit / Skip

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/sort/
    private static IEnumerable<BsonDocument> ExecuteSort(IEnumerable<BsonDocument> input, BsonDocument sortSpec)
    {
        return BsonSortEvaluator.Apply(input.ToList(), sortSpec);
    }

    #endregion

    #region $unwind

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/unwind/
    //   "Deconstructs an array field from the input documents to output a document for each element."
    private static IEnumerable<BsonDocument> ExecuteUnwind(IEnumerable<BsonDocument> input, BsonValue spec)
    {
        string path;
        bool preserveNullAndEmpty = false;
        string? includeArrayIndex = null;

        if (spec is BsonString s)
        {
            path = s.Value.TrimStart('$');
        }
        else
        {
            var doc = spec.AsBsonDocument;
            path = doc["path"].AsString.TrimStart('$');
            preserveNullAndEmpty = doc.GetValue("preserveNullAndEmptyArrays", false).AsBoolean;
            includeArrayIndex = doc.Contains("includeArrayIndex") && doc["includeArrayIndex"] != BsonNull.Value
                ? doc["includeArrayIndex"].AsString : null;
        }

        foreach (var doc in input)
        {
            var value = BsonFilterEvaluator.ResolveFieldPath(doc, path);

            if (value is BsonArray array && array.Count > 0)
            {
                for (int i = 0; i < array.Count; i++)
                {
                    var clone = doc.DeepClone().AsBsonDocument;
                    SetFieldPath(clone, path, array[i]);
                    if (includeArrayIndex != null)
                        clone[includeArrayIndex] = new BsonInt64(i);
                    yield return clone;
                }
            }
            else if (preserveNullAndEmpty)
            {
                var clone = doc.DeepClone().AsBsonDocument;
                if (value is BsonArray { Count: 0 })
                    clone.Remove(path);
                if (includeArrayIndex != null)
                    clone[includeArrayIndex] = BsonNull.Value;
                yield return clone;
            }
            // else: drop the document (no array or null/missing without preserve)
        }
    }

    #endregion

    #region $lookup

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/lookup/
    //   "Performs a left outer join to a collection in the same database."
    private static IEnumerable<BsonDocument> ExecuteLookup(
        IEnumerable<BsonDocument> input, BsonDocument spec, AggregationContext context)
    {
        var asField = spec["as"].AsString;

        // Determine foreign collection source
        IEnumerable<BsonDocument> GetForeignDocs()
        {
            if (spec["from"].IsBsonDocument)
            {
                // Cross-database lookup: { from: { db: "otherDb", coll: "users" } }
                var fromSpec = spec["from"].AsBsonDocument;
                return context.GetCollectionDocuments(fromSpec["db"].AsString, fromSpec["coll"].AsString);
            }
            return context.GetCollectionDocuments(spec["from"].AsString);
        }

        if (spec.Contains("pipeline"))
        {
            // Pipeline form
            var letVars = spec.GetValue("let", new BsonDocument()).AsBsonDocument;
            var pipeline = spec["pipeline"].AsBsonArray.Select(s => s.AsBsonDocument).ToList();

            foreach (var doc in input)
            {
                var vars = new BsonDocument();
                foreach (var letVar in letVars)
                    vars[letVar.Name] = AggregationExpressionEvaluator.Evaluate(doc, letVar.Value);

                var subContext = context.WithVariables(vars);
                var foreignInput = GetForeignDocs().Select(d => d.DeepClone().AsBsonDocument);
                var subResult = Execute(foreignInput, pipeline, subContext).ToList();

                var clone = doc.DeepClone().AsBsonDocument;
                clone[asField] = new BsonArray(subResult);
                yield return clone;
            }
        }
        else
        {
            // Simple equality form
            var localField = spec["localField"].AsString;
            var foreignField = spec["foreignField"].AsString;
            var foreignDocs = GetForeignDocs().ToList();

            foreach (var doc in input)
            {
                var localValue = BsonFilterEvaluator.ResolveFieldPath(doc, localField);
                var matches = foreignDocs
                    .Where(f =>
                    {
                        var fVal = BsonFilterEvaluator.ResolveFieldPath(f, foreignField);
                        if (localValue is BsonArray localArr)
                            return localArr.Contains(fVal);
                        if (fVal is BsonArray foreignArr)
                            return foreignArr.Contains(localValue);
                        return BsonValueComparer.Instance.Equals(fVal, localValue);
                    })
                    .Select(f => f.DeepClone().AsBsonDocument)
                    .ToList();

                var clone = doc.DeepClone().AsBsonDocument;
                clone[asField] = new BsonArray(matches);
                yield return clone;
            }
        }
    }

    #endregion

    #region Replace / Count / Sample

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/replaceRoot/
    private static IEnumerable<BsonDocument> ExecuteReplaceRoot(IEnumerable<BsonDocument> input, BsonDocument spec)
    {
        var newRootExpr = spec["newRoot"];
        return input.Select(doc =>
        {
            var newRoot = AggregationExpressionEvaluator.Evaluate(doc, newRootExpr);
            if (!newRoot.IsBsonDocument)
                throw MongoErrors.BadValue("$replaceRoot: 'newRoot' expression must evaluate to a document");
            return newRoot.AsBsonDocument;
        });
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/replaceWith/
    //   "Alias for $replaceRoot."
    private static IEnumerable<BsonDocument> ExecuteReplaceWith(IEnumerable<BsonDocument> input, BsonValue spec)
    {
        return input.Select(doc =>
        {
            var newRoot = AggregationExpressionEvaluator.Evaluate(doc, spec);
            if (!newRoot.IsBsonDocument)
                throw MongoErrors.BadValue("$replaceWith: expression must evaluate to a document");
            return newRoot.AsBsonDocument;
        });
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/count/
    //   "Passes a document to the next stage that contains a count of the number of documents."
    private static IEnumerable<BsonDocument> ExecuteCount(IEnumerable<BsonDocument> input, string fieldName)
    {
        var count = input.Count();
        yield return new BsonDocument(fieldName, count);
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/sortByCount/
    //   "Groups incoming documents based on the value of a specified expression,
    //    then computes the count of documents in each distinct group."
    private static IEnumerable<BsonDocument> ExecuteSortByCount(IEnumerable<BsonDocument> input, BsonValue expr)
    {
        var docs = input.ToList();
        var groups = docs.GroupBy(
            doc => AggregationExpressionEvaluator.Evaluate(doc, expr),
            BsonValueComparer.Instance);

        return groups
            .Select(g => new BsonDocument { { "_id", g.Key }, { "count", g.Count() } })
            .OrderByDescending(d => d["count"].AsInt32)
            .ThenBy(d => d["_id"], BsonValueComparer.Instance);
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/sample/
    //   "Randomly selects the specified number of documents from its input."
    private static IEnumerable<BsonDocument> ExecuteSample(IEnumerable<BsonDocument> input, BsonDocument spec)
    {
        var size = spec["size"].AsInt32;
        var docs = input.ToList();
        var rng = new Random();
        // Fisher-Yates shuffle for unbiased sampling
        for (int i = docs.Count - 1; i > 0; i--)
        {
            int j = rng.Next(i + 1);
            (docs[i], docs[j]) = (docs[j], docs[i]);
        }
        return docs.Take(size);
    }

    #endregion

    #region $facet

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/facet/
    //   "Processes multiple aggregation pipelines within a single stage on the same set of input documents."
    private static IEnumerable<BsonDocument> ExecuteFacet(
        IEnumerable<BsonDocument> input, BsonDocument spec, AggregationContext context)
    {
        var docs = input.ToList();
        var result = new BsonDocument();

        foreach (var facet in spec)
        {
            var pipeline = facet.Value.AsBsonArray.Select(s => s.AsBsonDocument).ToList();
            var facetResult = Execute(docs, pipeline, context).ToList();
            result[facet.Name] = new BsonArray(facetResult);
        }

        yield return result;
    }

    #endregion

    #region $bucket / $bucketAuto

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/bucket/
    private static IEnumerable<BsonDocument> ExecuteBucket(IEnumerable<BsonDocument> input, BsonDocument spec)
    {
        var groupBy = spec["groupBy"];
        var boundaries = spec["boundaries"].AsBsonArray;
        var defaultBucket = spec.GetValue("default", BsonNull.Value);
        var outputSpec = spec.Contains("output") ? spec["output"].AsBsonDocument : null;

        var docs = input.ToList();
        var buckets = new Dictionary<int, List<BsonDocument>>();
        var defaultDocs = new List<BsonDocument>();

        foreach (var doc in docs)
        {
            var val = AggregationExpressionEvaluator.Evaluate(doc, groupBy);
            bool placed = false;
            for (int i = 0; i < boundaries.Count - 1; i++)
            {
                if (BsonValueComparer.Instance.Compare(val, boundaries[i]) >= 0 &&
                    BsonValueComparer.Instance.Compare(val, boundaries[i + 1]) < 0)
                {
                    if (!buckets.ContainsKey(i)) buckets[i] = new List<BsonDocument>();
                    buckets[i].Add(doc);
                    placed = true;
                    break;
                }
            }
            if (!placed)
            {
                if (defaultBucket != BsonNull.Value) defaultDocs.Add(doc);
                else throw MongoErrors.BadValue("$bucket: element doesn't fall into any bucket and no default specified");
            }
        }

        for (int i = 0; i < boundaries.Count - 1; i++)
        {
            if (!buckets.ContainsKey(i)) continue;
            var result = new BsonDocument { { "_id", boundaries[i] } };
            if (outputSpec != null)
                ApplyBucketOutput(result, buckets[i], outputSpec);
            else
                result["count"] = buckets[i].Count;
            yield return result;
        }

        if (defaultDocs.Count > 0)
        {
            var result = new BsonDocument { { "_id", defaultBucket } };
            if (outputSpec != null)
                ApplyBucketOutput(result, defaultDocs, outputSpec);
            else
                result["count"] = defaultDocs.Count;
            yield return result;
        }
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/bucketAuto/
    private static IEnumerable<BsonDocument> ExecuteBucketAuto(IEnumerable<BsonDocument> input, BsonDocument spec)
    {
        var groupBy = spec["groupBy"];
        var buckets = spec["buckets"].AsInt32;

        var docs = input.ToList();
        var sorted = docs.OrderBy(d => AggregationExpressionEvaluator.Evaluate(d, groupBy), BsonValueComparer.Instance).ToList();
        int perBucket = Math.Max(1, (int)Math.Ceiling((double)sorted.Count / buckets));

        for (int i = 0; i < sorted.Count; i += perBucket)
        {
            var chunk = sorted.Skip(i).Take(perBucket).ToList();
            if (chunk.Count == 0) continue;
            var minVal = AggregationExpressionEvaluator.Evaluate(chunk[0], groupBy);
            var maxIdx = Math.Min(i + perBucket, sorted.Count);
            var maxVal = maxIdx < sorted.Count
                ? AggregationExpressionEvaluator.Evaluate(sorted[maxIdx], groupBy)
                : AggregationExpressionEvaluator.Evaluate(chunk[^1], groupBy);

            var result = new BsonDocument
            {
                { "_id", new BsonDocument { { "min", minVal }, { "max", maxVal } } },
                { "count", chunk.Count }
            };
            yield return result;
        }
    }

    private static void ApplyBucketOutput(BsonDocument result, List<BsonDocument> docs, BsonDocument outputSpec)
    {
        foreach (var field in outputSpec)
        {
            var accSpec = field.Value.AsBsonDocument;
            var op = accSpec.Names.First();
            var fieldExpr = accSpec[op];
            result[field.Name] = op switch
            {
                "$sum" => ComputeSum(docs, fieldExpr),
                "$avg" => ComputeAvg(docs, fieldExpr),
                "$min" => ComputeMin(docs, fieldExpr),
                "$max" => ComputeMax(docs, fieldExpr),
                "$first" => ComputeFirst(docs, fieldExpr),
                "$last" => ComputeLast(docs, fieldExpr),
                "$push" => new BsonArray(docs.Select(d => AggregationExpressionEvaluator.Evaluate(d, fieldExpr))),
                "$addToSet" => new BsonArray(docs.Select(d => AggregationExpressionEvaluator.Evaluate(d, fieldExpr)).Distinct(BsonValueComparer.Instance)),
                _ => throw new NotSupportedException($"Bucket accumulator '{op}' not supported")
            };
        }
    }

    #endregion

    #region $unionWith

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/unionWith/
    //   "Performs a union of two collections."
    private static IEnumerable<BsonDocument> ExecuteUnionWith(
        IEnumerable<BsonDocument> input, BsonDocument spec, AggregationContext context)
    {
        var coll = spec["coll"].AsString;
        var foreignDocs = context.GetCollectionDocuments(coll).Select(d => d.DeepClone().AsBsonDocument);

        if (spec.Contains("pipeline"))
        {
            var pipeline = spec["pipeline"].AsBsonArray.Select(s => s.AsBsonDocument).ToList();
            foreignDocs = Execute(foreignDocs, pipeline, context);
        }

        return input.Concat(foreignDocs);
    }

    #endregion

    #region $graphLookup

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/graphLookup/
    //   "Performs a recursive search on a collection."
    private static IEnumerable<BsonDocument> ExecuteGraphLookup(
        IEnumerable<BsonDocument> input, BsonDocument spec, AggregationContext context)
    {
        var from = spec["from"].AsString;
        var startWith = spec["startWith"];
        var connectFromField = spec["connectFromField"].AsString;
        var connectToField = spec["connectToField"].AsString;
        var asField = spec["as"].AsString;
        var maxDepth = spec.Contains("maxDepth") ? spec["maxDepth"].AsInt32 : int.MaxValue;
        var depthField = spec.Contains("depthField") ? spec["depthField"].AsString : null;
        var restrictSearchWithMatch = spec.Contains("restrictSearchWithMatch")
            ? spec["restrictSearchWithMatch"].AsBsonDocument : null;

        var foreignDocs = context.GetCollectionDocuments(from).ToList();

        foreach (var doc in input)
        {
            var startValue = AggregationExpressionEvaluator.Evaluate(doc, startWith);
            var visited = new HashSet<BsonValue>(BsonValueComparer.Instance);
            var results = new BsonArray();
            var frontier = new List<(BsonValue value, int depth)>();

            // Initialize frontier
            if (startValue is BsonArray arr)
                foreach (var v in arr) frontier.Add((v, 0));
            else
                frontier.Add((startValue, 0));

            while (frontier.Count > 0)
            {
                var nextFrontier = new List<(BsonValue value, int depth)>();

                foreach (var (val, depth) in frontier)
                {
                    if (depth > maxDepth) continue;
                    if (!visited.Add(val)) continue;

                    foreach (var foreign in foreignDocs)
                    {
                        var toVal = BsonFilterEvaluator.ResolveFieldPath(foreign, connectToField);
                        bool matches = toVal is BsonArray toArr ? toArr.Contains(val) : BsonValueComparer.Instance.Equals(toVal, val);
                        if (!matches) continue;
                        if (restrictSearchWithMatch != null && !BsonFilterEvaluator.Matches(foreign, restrictSearchWithMatch))
                            continue;

                        var clone = foreign.DeepClone().AsBsonDocument;
                        if (depthField != null)
                            clone[depthField] = new BsonInt64(depth);
                        results.Add(clone);

                        var fromVal = BsonFilterEvaluator.ResolveFieldPath(foreign, connectFromField);
                        if (fromVal is BsonArray fromArr)
                            foreach (var fv in fromArr) nextFrontier.Add((fv, depth + 1));
                        else if (fromVal != BsonNull.Value)
                            nextFrontier.Add((fromVal, depth + 1));
                    }
                }

                frontier = nextFrontier;
            }

            var docClone = doc.DeepClone().AsBsonDocument;
            docClone[asField] = results;
            yield return docClone;
        }
    }

    #endregion

    #region $redact

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/redact/
    //   "Restricts the contents of the documents based on information stored in the documents themselves."
    private static IEnumerable<BsonDocument> ExecuteRedact(IEnumerable<BsonDocument> input, BsonValue expr)
    {
        foreach (var doc in input)
        {
            var result = RedactDocument(doc, expr);
            if (result != null) yield return result;
        }
    }

    private static BsonDocument? RedactDocument(BsonDocument doc, BsonValue expr)
    {
        var action = AggregationExpressionEvaluator.Evaluate(doc, expr);
        var actionStr = action.AsString;

        if (actionStr == "$$PRUNE") return null;
        if (actionStr == "$$KEEP") return doc.DeepClone().AsBsonDocument;
        if (actionStr == "$$DESCEND")
        {
            var clone = new BsonDocument();
            foreach (var el in doc)
            {
                if (el.Value.IsBsonDocument)
                {
                    var sub = RedactDocument(el.Value.AsBsonDocument, expr);
                    if (sub != null) clone[el.Name] = sub;
                }
                else if (el.Value.IsBsonArray)
                {
                    var arr = new BsonArray();
                    foreach (var item in el.Value.AsBsonArray)
                    {
                        if (item.IsBsonDocument)
                        {
                            var sub = RedactDocument(item.AsBsonDocument, expr);
                            if (sub != null) arr.Add(sub);
                        }
                        else
                        {
                            arr.Add(item);
                        }
                    }
                    clone[el.Name] = arr;
                }
                else
                {
                    clone[el.Name] = el.Value;
                }
            }
            return clone;
        }

        return doc.DeepClone().AsBsonDocument;
    }

    #endregion

    #region $merge / $out

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/merge/
    //   "Writes the results of the aggregation pipeline to a specified collection."
    private static IEnumerable<BsonDocument> ExecuteMerge(
        IEnumerable<BsonDocument> input, BsonDocument spec, AggregationContext context)
    {
        string targetCollection;
        if (spec["into"].IsString)
            targetCollection = spec["into"].AsString;
        else
            targetCollection = spec["into"].AsBsonDocument["coll"].AsString;

        var onFields = spec.Contains("on")
            ? (spec["on"] is BsonArray arr
                ? arr.Select(v => v.AsString).ToList()
                : new List<string> { spec["on"].AsString })
            : new List<string> { "_id" };

        var whenMatched = spec.GetValue("whenMatched", "merge").AsString;
        var whenNotMatched = spec.GetValue("whenNotMatched", "insert").AsString;

        var store = context.GetOrCreateStore(targetCollection);
        var results = input.ToList();

        foreach (var doc in results)
        {
            var existing = store.GetAll().FirstOrDefault(d =>
                onFields.All(f => BsonValueComparer.Instance.Equals(
                    BsonFilterEvaluator.ResolveFieldPath(d, f),
                    BsonFilterEvaluator.ResolveFieldPath(doc, f))));

            if (existing != null)
            {
                var id = existing["_id"];
                switch (whenMatched)
                {
                    case "replace":
                        store.Replace(id, doc.DeepClone().AsBsonDocument);
                        break;
                    case "keepExisting":
                        break; // do nothing
                    case "merge":
                        var merged = existing.DeepClone().AsBsonDocument;
                        foreach (var el in doc) merged[el.Name] = el.Value;
                        store.Replace(id, merged);
                        break;
                    case "fail":
                        throw MongoErrors.DuplicateKey(doc.GetValue("_id", BsonNull.Value));
                }
            }
            else
            {
                switch (whenNotMatched)
                {
                    case "insert":
                        DocumentStore.EnsureId(doc);
                        store.Insert(doc.DeepClone().AsBsonDocument);
                        break;
                    case "discard":
                        break;
                    case "fail":
                        throw MongoErrors.BadValue("$merge: document without match and whenNotMatched is 'fail'");
                }
            }
        }

        return results;
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/out/
    //   "Takes the documents returned by the aggregation pipeline and writes them to a specified collection."
    private static IEnumerable<BsonDocument> ExecuteOut(
        IEnumerable<BsonDocument> input, BsonValue spec, AggregationContext context)
    {
        var targetCollection = spec.IsString ? spec.AsString : spec.AsBsonDocument["coll"].AsString;
        var results = input.ToList();

        var store = context.GetOrCreateStore(targetCollection);
        store.Clear();

        foreach (var doc in results)
        {
            var clone = doc.DeepClone().AsBsonDocument;
            DocumentStore.EnsureId(clone);
            store.Insert(clone);
        }

        return results;
    }

    #endregion

    #region $setWindowFields

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/setWindowFields/
    //   "Performs operations on a specified span of documents in a collection, known as a window."
    private static IEnumerable<BsonDocument> ExecuteSetWindowFields(IEnumerable<BsonDocument> input, BsonDocument spec)
    {
        var partitionExpr = spec.GetValue("partitionBy", BsonNull.Value);
        var sortSpec = spec.Contains("sortBy") ? spec["sortBy"].AsBsonDocument : new BsonDocument();
        var outputSpec = spec["output"].AsBsonDocument;

        var docs = input.ToList();

        // Partition
        IEnumerable<IGrouping<BsonValue, BsonDocument>> partitions;
        if (partitionExpr != BsonNull.Value)
            partitions = docs.GroupBy(d => AggregationExpressionEvaluator.Evaluate(d, partitionExpr), BsonValueComparer.Instance);
        else
            partitions = new[] { docs.GroupBy(_ => (BsonValue)BsonNull.Value, BsonValueComparer.Instance).First() };

        foreach (var partition in partitions)
        {
            var sorted = sortSpec.ElementCount > 0
                ? BsonSortEvaluator.Apply(partition.ToList(), sortSpec).ToList()
                : partition.ToList();

            for (int i = 0; i < sorted.Count; i++)
            {
                var clone = sorted[i].DeepClone().AsBsonDocument;

                foreach (var field in outputSpec)
                {
                    var windowSpec = field.Value.AsBsonDocument;
                    var op = windowSpec.Names.First();
                    var opArgs = windowSpec[op];
                    var window = windowSpec.Contains("window") ? windowSpec["window"].AsBsonDocument : null;

                    clone[field.Name] = op switch
                    {
                        "$sum" => ComputeWindowAggregate(sorted, i, opArgs, window, vals => new BsonDouble(vals.Where(v => v.IsNumeric).Sum(v => v.ToDouble()))),
                        "$avg" => ComputeWindowAggregate(sorted, i, opArgs, window, vals => { var nums = vals.Where(v => v.IsNumeric).ToList(); return nums.Count > 0 ? new BsonDouble(nums.Average(v => v.ToDouble())) : BsonNull.Value; }),
                        "$min" => ComputeWindowAggregate(sorted, i, opArgs, window, vals => { var valid = vals.Where(v => v != BsonNull.Value).ToList(); return valid.Count > 0 ? valid.Min(BsonValueComparer.Instance)! : BsonNull.Value; }),
                        "$max" => ComputeWindowAggregate(sorted, i, opArgs, window, vals => { var valid = vals.Where(v => v != BsonNull.Value).ToList(); return valid.Count > 0 ? valid.Max(BsonValueComparer.Instance)! : BsonNull.Value; }),
                        "$count" => ComputeWindowAggregate(sorted, i, new BsonDocument(), window, vals => new BsonInt32(vals.Count)),
                        "$push" => ComputeWindowAggregate(sorted, i, opArgs, window, vals => new BsonArray(vals)),
                        "$addToSet" => ComputeWindowAggregate(sorted, i, opArgs, window, vals => new BsonArray(vals.Distinct(BsonValueComparer.Instance).ToList())),
                        "$first" => ComputeWindowAggregate(sorted, i, opArgs, window, vals => vals.Count > 0 ? vals[0] : BsonNull.Value),
                        "$last" => ComputeWindowAggregate(sorted, i, opArgs, window, vals => vals.Count > 0 ? vals[^1] : BsonNull.Value),
                        "$stdDevPop" => ComputeWindowAggregate(sorted, i, opArgs, window, vals =>
                        {
                            var nums = vals.Where(v => v.IsNumeric).Select(v => v.ToDouble()).ToList();
                            if (nums.Count == 0) return BsonNull.Value;
                            var mean = nums.Average();
                            return new BsonDouble(Math.Sqrt(nums.Sum(v => (v - mean) * (v - mean)) / nums.Count));
                        }),
                        "$stdDevSamp" => ComputeWindowAggregate(sorted, i, opArgs, window, vals =>
                        {
                            var nums = vals.Where(v => v.IsNumeric).Select(v => v.ToDouble()).ToList();
                            if (nums.Count < 2) return BsonNull.Value;
                            var mean = nums.Average();
                            return new BsonDouble(Math.Sqrt(nums.Sum(v => (v - mean) * (v - mean)) / (nums.Count - 1)));
                        }),
                        "$rank" => new BsonInt32(ComputeRank(sorted, i, sortSpec)),
                        "$denseRank" => new BsonInt32(ComputeDenseRank(sorted, i, sortSpec)),
                        "$documentNumber" => new BsonInt32(i + 1),
                        "$shift" => ComputeShift(sorted, i, windowSpec),
                        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/expMovingAvg/
                        "$expMovingAvg" => ComputeExpMovingAvg(sorted, i, windowSpec),
                        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/derivative/
                        "$derivative" => ComputeDerivative(sorted, i, windowSpec),
                        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/integral/
                        "$integral" => ComputeIntegral(sorted, i, windowSpec, window),
                        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/covariancePop/
                        "$covariancePop" => ComputeCovariance(sorted, i, windowSpec, window, population: true),
                        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/covarianceSamp/
                        "$covarianceSamp" => ComputeCovariance(sorted, i, windowSpec, window, population: false),
                        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/linearFill/
                        "$linearFill" => ComputeLinearFill(sorted, i, opArgs),
                        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/locf/
                        "$locf" => ComputeLocf(sorted, i, opArgs),
                        _ => throw new NotSupportedException($"Window function '{op}' is not supported.")
                    };
                }

                yield return clone;
            }
        }
    }

    private static BsonValue ComputeWindowAggregate(
        List<BsonDocument> sorted, int currentIdx, BsonValue fieldExpr,
        BsonDocument? window, Func<List<BsonValue>, BsonValue> aggregate)
    {
        var (start, end) = ResolveWindow(window, currentIdx, sorted.Count);
        var values = new List<BsonValue>();
        for (int i = start; i <= end; i++)
        {
            if (fieldExpr is BsonDocument emptyDoc && emptyDoc.ElementCount == 0)
                values.Add(BsonNull.Value); // $count uses empty doc
            else
                values.Add(AggregationExpressionEvaluator.Evaluate(sorted[i], fieldExpr));
        }
        return aggregate(values);
    }

    private static (int start, int end) ResolveWindow(BsonDocument? window, int currentIdx, int total)
    {
        if (window == null)
            return (0, total - 1); // entire partition

        if (window.Contains("documents"))
        {
            var docs = window["documents"].AsBsonArray;
            int start = ResolveWindowBound(docs[0], currentIdx, total, isLower: true);
            int end = ResolveWindowBound(docs[1], currentIdx, total, isLower: false);
            return (Math.Max(0, start), Math.Min(total - 1, end));
        }

        // Range-based windows (simplified: treat as document-based)
        if (window.Contains("range"))
        {
            var range = window["range"].AsBsonArray;
            int start = ResolveWindowBound(range[0], currentIdx, total, isLower: true);
            int end = ResolveWindowBound(range[1], currentIdx, total, isLower: false);
            return (Math.Max(0, start), Math.Min(total - 1, end));
        }

        return (0, total - 1);
    }

    private static int ResolveWindowBound(BsonValue bound, int currentIdx, int total, bool isLower)
    {
        if (bound.IsString)
        {
            return bound.AsString switch
            {
                "unbounded" => isLower ? 0 : total - 1,
                "current" => currentIdx,
                _ => currentIdx
            };
        }
        return currentIdx + bound.ToInt32();
    }

    private static int ComputeRank(List<BsonDocument> sorted, int idx, BsonDocument sortSpec)
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/rank/
        //   "Returns the document position (known as the rank) relative to other documents."
        //   Rank = 1 + number of rows that come before the first row of the current peer group.
        //   Tied rows have the same rank; the next rank after a tie skips.
        for (int i = idx - 1; i >= 0; i--)
        {
            if (!SortKeysEqual(sorted[i], sorted[idx], sortSpec))
                return idx + 1; // Not in same group as predecessor: rank is position + 1
        }
        // All preceding rows are peers (or idx == 0): rank is 1
        // But we need to find where our peer group starts
        int groupStart = 0;
        for (int i = 0; i < idx; i++)
        {
            if (!SortKeysEqual(sorted[i], sorted[idx], sortSpec))
                groupStart = i + 1;
        }
        return groupStart + 1;
    }

    private static int ComputeDenseRank(List<BsonDocument> sorted, int idx, BsonDocument sortSpec)
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/denseRank/
        int rank = 1;
        for (int i = 1; i <= idx; i++)
        {
            if (!SortKeysEqual(sorted[i - 1], sorted[i], sortSpec))
                rank++;
        }
        return rank;
    }

    private static bool SortKeysEqual(BsonDocument a, BsonDocument b, BsonDocument sortSpec)
    {
        foreach (var el in sortSpec)
        {
            var va = BsonFilterEvaluator.ResolveFieldPath(a, el.Name);
            var vb = BsonFilterEvaluator.ResolveFieldPath(b, el.Name);
            if (BsonValueComparer.Instance.Compare(va, vb) != 0)
                return false;
        }
        return true;
    }

    private static BsonValue ComputeShift(List<BsonDocument> sorted, int idx, BsonDocument spec)
    {
        var shiftSpec = spec["$shift"].AsBsonDocument;
        var output = shiftSpec["output"];
        var by = shiftSpec["by"].ToInt32();
        var defaultValue = shiftSpec.GetValue("default", BsonNull.Value);
        var targetIdx = idx + by;
        if (targetIdx < 0 || targetIdx >= sorted.Count)
            return defaultValue;
        return AggregationExpressionEvaluator.Evaluate(sorted[targetIdx], output);
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/expMovingAvg/
    //   "Returns the exponential moving average of numeric expressions applied to documents
    //    in a partition defined in the $setWindowFields stage."
    private static BsonValue ComputeExpMovingAvg(List<BsonDocument> sorted, int idx, BsonDocument spec)
    {
        var emaSpec = spec["$expMovingAvg"].AsBsonDocument;
        var input = emaSpec["input"];

        // alpha can be provided directly or calculated from N: alpha = 2/(N+1)
        double alpha;
        if (emaSpec.Contains("alpha"))
            alpha = emaSpec["alpha"].ToDouble();
        else
        {
            var n = emaSpec["N"].ToInt32();
            alpha = 2.0 / (n + 1);
        }

        // Compute EMA from the beginning of the partition up to current index
        double? ema = null;
        for (int i = 0; i <= idx; i++)
        {
            var val = AggregationExpressionEvaluator.Evaluate(sorted[i], input);
            if (!val.IsNumeric) continue;
            var v = val.ToDouble();
            ema = ema == null ? v : alpha * v + (1 - alpha) * ema.Value;
        }

        return ema.HasValue ? new BsonDouble(ema.Value) : BsonNull.Value;
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/derivative/
    //   "Returns the average rate of change within the specified window."
    private static BsonValue ComputeDerivative(List<BsonDocument> sorted, int idx, BsonDocument spec)
    {
        var derivSpec = spec["$derivative"].AsBsonDocument;
        var input = derivSpec["input"];

        if (idx == 0) return BsonNull.Value;

        var currVal = AggregationExpressionEvaluator.Evaluate(sorted[idx], input);
        var prevVal = AggregationExpressionEvaluator.Evaluate(sorted[idx - 1], input);

        if (!currVal.IsNumeric || !prevVal.IsNumeric) return BsonNull.Value;

        // Simple derivative: change from previous to current
        return new BsonDouble(currVal.ToDouble() - prevVal.ToDouble());
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/integral/
    //   "Returns the approximation of the area under a curve."
    private static BsonValue ComputeIntegral(List<BsonDocument> sorted, int idx, BsonDocument spec, BsonDocument? window)
    {
        var intSpec = spec["$integral"].AsBsonDocument;
        var input = intSpec["input"];

        var (start, end) = ResolveWindow(window, idx, sorted.Count);
        if (start >= end) return new BsonDouble(0);

        // Trapezoidal rule using index positions
        double area = 0;
        for (int i = start; i < end; i++)
        {
            var v0 = AggregationExpressionEvaluator.Evaluate(sorted[i], input);
            var v1 = AggregationExpressionEvaluator.Evaluate(sorted[i + 1], input);
            if (!v0.IsNumeric || !v1.IsNumeric) continue;
            area += (v0.ToDouble() + v1.ToDouble()) / 2.0;
        }

        return new BsonDouble(area);
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/covariancePop/
    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/covarianceSamp/
    private static BsonValue ComputeCovariance(List<BsonDocument> sorted, int idx, BsonDocument spec, BsonDocument? window, bool population)
    {
        var opName = population ? "$covariancePop" : "$covarianceSamp";
        var covArgs = spec[opName].AsBsonArray;
        var expr1 = covArgs[0];
        var expr2 = covArgs[1];

        var (start, end) = ResolveWindow(window, idx, sorted.Count);

        var pairs = new List<(double x, double y)>();
        for (int i = start; i <= end; i++)
        {
            var v1 = AggregationExpressionEvaluator.Evaluate(sorted[i], expr1);
            var v2 = AggregationExpressionEvaluator.Evaluate(sorted[i], expr2);
            if (v1.IsNumeric && v2.IsNumeric)
                pairs.Add((v1.ToDouble(), v2.ToDouble()));
        }

        if (pairs.Count == 0) return BsonNull.Value;
        if (!population && pairs.Count < 2) return BsonNull.Value;

        var meanX = pairs.Average(p => p.x);
        var meanY = pairs.Average(p => p.y);
        var cov = pairs.Sum(p => (p.x - meanX) * (p.y - meanY)) / (population ? pairs.Count : pairs.Count - 1);
        return new BsonDouble(cov);
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/linearFill/
    //   "Fills null and missing fields in a window using linear interpolation."
    private static BsonValue ComputeLinearFill(List<BsonDocument> sorted, int idx, BsonValue fieldExpr)
    {
        var val = AggregationExpressionEvaluator.Evaluate(sorted[idx], fieldExpr);
        if (val != BsonNull.Value && val.BsonType != BsonType.Undefined) return val;

        // Find previous non-null
        int prevIdx = -1;
        BsonValue prevVal = BsonNull.Value;
        for (int i = idx - 1; i >= 0; i--)
        {
            var v = AggregationExpressionEvaluator.Evaluate(sorted[i], fieldExpr);
            if (v != BsonNull.Value && v.BsonType != BsonType.Undefined && v.IsNumeric)
            {
                prevIdx = i;
                prevVal = v;
                break;
            }
        }

        // Find next non-null
        int nextIdx = -1;
        BsonValue nextVal = BsonNull.Value;
        for (int i = idx + 1; i < sorted.Count; i++)
        {
            var v = AggregationExpressionEvaluator.Evaluate(sorted[i], fieldExpr);
            if (v != BsonNull.Value && v.BsonType != BsonType.Undefined && v.IsNumeric)
            {
                nextIdx = i;
                nextVal = v;
                break;
            }
        }

        if (prevIdx < 0 || nextIdx < 0) return BsonNull.Value;

        // Linear interpolation
        double fraction = (double)(idx - prevIdx) / (nextIdx - prevIdx);
        double result = prevVal.ToDouble() + fraction * (nextVal.ToDouble() - prevVal.ToDouble());
        return new BsonDouble(result);
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/locf/
    //   "Last observation carried forward. Sets values for null and missing fields to the last non-null value."
    private static BsonValue ComputeLocf(List<BsonDocument> sorted, int idx, BsonValue fieldExpr)
    {
        var val = AggregationExpressionEvaluator.Evaluate(sorted[idx], fieldExpr);
        if (val != BsonNull.Value && val.BsonType != BsonType.Undefined) return val;

        // Find previous non-null value
        for (int i = idx - 1; i >= 0; i--)
        {
            var v = AggregationExpressionEvaluator.Evaluate(sorted[i], fieldExpr);
            if (v != BsonNull.Value && v.BsonType != BsonType.Undefined) return v;
        }

        return BsonNull.Value;
    }

    #endregion

    #region $densify / $fill

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/densify/
    //   "Creates new documents in a sequence of documents where certain values are missing."
    private static IEnumerable<BsonDocument> ExecuteDensify(IEnumerable<BsonDocument> input, BsonDocument spec)
    {
        var field = spec["field"].AsString;
        var rangeSpec = spec["range"].AsBsonDocument;
        var step = rangeSpec["step"].ToDouble();
        var bounds = rangeSpec.GetValue("bounds", "full").AsString;

        var docs = input.ToList();
        if (docs.Count == 0) return docs;

        var sorted = docs.OrderBy(d => BsonFilterEvaluator.ResolveFieldPath(d, field), BsonValueComparer.Instance).ToList();
        var result = new List<BsonDocument>();

        for (int i = 0; i < sorted.Count; i++)
        {
            result.Add(sorted[i]);
            if (i < sorted.Count - 1)
            {
                var currentVal = BsonFilterEvaluator.ResolveFieldPath(sorted[i], field).ToDouble();
                var nextVal = BsonFilterEvaluator.ResolveFieldPath(sorted[i + 1], field).ToDouble();
                var fillVal = currentVal + step;
                while (fillVal < nextVal)
                {
                    result.Add(new BsonDocument { { field, fillVal } });
                    fillVal += step;
                }
            }
        }

        return result;
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/fill/
    //   "Populates null and missing field values within documents."
    private static IEnumerable<BsonDocument> ExecuteFill(IEnumerable<BsonDocument> input, BsonDocument spec)
    {
        var outputSpec = spec["output"].AsBsonDocument;
        var sortBy = spec.Contains("sortBy") ? spec["sortBy"].AsBsonDocument : null;

        var docs = input.ToList();
        if (sortBy != null)
            docs = BsonSortEvaluator.Apply(docs, sortBy).ToList();

        var result = new List<BsonDocument>();
        var lastValues = new Dictionary<string, BsonValue>();

        foreach (var doc in docs)
        {
            var clone = doc.DeepClone().AsBsonDocument;
            foreach (var field in outputSpec)
            {
                var fillSpec = field.Value.AsBsonDocument;
                var val = BsonFilterEvaluator.ResolveFieldPath(clone, field.Name);
                if (val == BsonNull.Value || !clone.Contains(field.Name))
                {
                    if (fillSpec.Contains("value"))
                    {
                        clone[field.Name] = fillSpec["value"];
                    }
                    else if (fillSpec.Contains("method"))
                    {
                        var method = fillSpec["method"].AsString;
                        if (method == "locf" && lastValues.ContainsKey(field.Name))
                            clone[field.Name] = lastValues[field.Name];
                    }
                }
                else
                {
                    lastValues[field.Name] = val;
                }
            }
            result.Add(clone);
        }

        return result;
    }

    #endregion

    #region $documents / Stats

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/documents/
    //   "Returns literal documents from input values."
    private static IEnumerable<BsonDocument> ExecuteDocuments(BsonArray docs)
    {
        return docs.Select(d => d.AsBsonDocument.DeepClone().AsBsonDocument);
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/collStats/
    private static IEnumerable<BsonDocument> ExecuteCollStats(BsonDocument spec, AggregationContext context)
    {
        yield return new BsonDocument
        {
            { "ns", context.CollectionNamespace ?? "unknown" },
            { "count", context.DocumentCount },
            { "size", context.DocumentCount * 256 },
            { "avgObjSize", 256 },
            { "storageSize", context.DocumentCount * 256 },
            { "totalSize", context.DocumentCount * 512 }
        };
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/indexStats/
    private static IEnumerable<BsonDocument> ExecuteIndexStats(AggregationContext context)
    {
        yield return new BsonDocument
        {
            { "name", "_id_" },
            { "key", new BsonDocument("_id", 1) },
            { "accesses", new BsonDocument { { "ops", 0L }, { "since", new BsonDateTime(DateTime.UtcNow) } } }
        };
    }

    #endregion

    #region GeoNear

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/geoNear/
    //   "Outputs documents in order of nearest to farthest from a specified point."
    //   "$geoNear must be the first stage in the pipeline."
    private static IEnumerable<BsonDocument> ExecuteGeoNear(IEnumerable<BsonDocument> input, BsonDocument spec)
    {
        var nearPoint = GeoJsonHelper.ExtractPoint(spec["near"]);
        if (nearPoint == null) return input;

        var distanceField = spec["distanceField"].AsString;
        var spherical = spec.GetValue("spherical", true).ToBoolean();
        var maxDistance = spec.Contains("maxDistance") ? spec["maxDistance"].ToDouble() : double.MaxValue;
        var minDistance = spec.Contains("minDistance") ? spec["minDistance"].ToDouble() : 0.0;
        var query = spec.Contains("query") ? spec["query"].AsBsonDocument : null;
        var key = spec.Contains("key") ? spec["key"].AsString : null;
        var includeLocs = spec.Contains("includeLocs") ? spec["includeLocs"].AsString : null;
        var distanceMultiplier = spec.Contains("distanceMultiplier") ? spec["distanceMultiplier"].ToDouble() : 1.0;

        var results = new List<(BsonDocument doc, double distance)>();

        foreach (var doc in input)
        {
            if (query != null && !BsonFilterEvaluator.Matches(doc, query))
                continue;

            // Find the geo field
            BsonValue? geoValue = null;
            if (key != null)
            {
                geoValue = BsonFilterEvaluator.ResolveFieldPath(doc, key);
            }
            else
            {
                // Auto-detect: find a field that looks like GeoJSON
                foreach (var el in doc)
                {
                    if (el.Value is BsonDocument geoDoc && geoDoc.Contains("type") && geoDoc.Contains("coordinates"))
                    {
                        geoValue = geoDoc;
                        break;
                    }
                }
            }

            if (geoValue == null || geoValue == BsonNull.Value) continue;

            var docPoint = GeoJsonHelper.ExtractPoint(geoValue);
            double dist;
            if (docPoint != null)
            {
                dist = GeoJsonHelper.HaversineDistance(nearPoint, docPoint);
            }
            else
            {
                var geom = GeoJsonHelper.ToGeometry(geoValue.AsBsonDocument);
                if (geom == null) continue;
                dist = GeoJsonHelper.DistanceMeters(nearPoint, geom);
            }

            if (dist < minDistance || dist > maxDistance) continue;
            results.Add((doc, dist));
        }

        results.Sort((a, b) => a.distance.CompareTo(b.distance));

        return results.Select(r =>
        {
            var result = r.doc.DeepClone().AsBsonDocument;
            SetFieldPath(result, distanceField, new BsonDouble(r.distance * distanceMultiplier));
            if (includeLocs != null)
            {
                var geoField = key != null
                    ? BsonFilterEvaluator.ResolveFieldPath(r.doc, key)
                    : r.doc.Elements.FirstOrDefault(e => e.Value is BsonDocument d && d.Contains("type") && d.Contains("coordinates")).Value;
                if (geoField != null)
                    SetFieldPath(result, includeLocs, geoField);
            }
            return result;
        }).ToList();
    }

    #endregion

    #region Helpers

    /// <summary>
    /// Set a field value using dot notation path, creating intermediate documents as needed.
    /// </summary>
    internal static void SetFieldPath(BsonDocument doc, string path, BsonValue value)
    {
        var parts = path.Split('.');
        var current = doc;
        for (int i = 0; i < parts.Length - 1; i++)
        {
            if (!current.Contains(parts[i]) || !current[parts[i]].IsBsonDocument)
                current[parts[i]] = new BsonDocument();
            current = current[parts[i]].AsBsonDocument;
        }
        current[parts[^1]] = value;
    }

    #endregion
}
