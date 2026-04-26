using Jint;
using Jint.Native;
using MongoDB.Bson;

namespace MongoDB.InMemoryEmulator.JsTriggers;

/// <summary>
/// Registers JavaScript expression support for the in-memory MongoDB emulator.
/// Enables $where, $function, and $accumulator operators via the Jint JavaScript engine.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/reference/operator/query/where/
/// Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/function/
/// Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/accumulator/
/// </remarks>
public static class JsExpressionSetup
{
    /// <summary>
    /// Registers JavaScript expression evaluators for $where, $function, and $accumulator.
    /// Call this once at test startup (e.g., in a test fixture constructor).
    /// </summary>
    public static void Register()
    {
        BsonFilterEvaluator.RegisterWhereEvaluator(EvaluateWhere);
        AggregationExpressionEvaluator.RegisterFunctionEvaluator(EvaluateFunction);
        AggregationExpressionEvaluator.RegisterAccumulatorEvaluator(EvaluateAccumulator);
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/where/
    //   "$where evaluates a JavaScript expression or function for each document."
    //   "In the function, 'this' refers to the document being evaluated."
    private static bool EvaluateWhere(BsonDocument document, BsonValue whereExpr)
    {
        var engine = CreateEngine();
        var jsObj = BsonToJsValue(engine, document);
        engine.SetValue("obj", jsObj);

        string script;
        if (whereExpr.IsString)
        {
            var jsCode = whereExpr.AsString;
            // Detect if it's a function body or an expression
            if (jsCode.TrimStart().StartsWith("function"))
            {
                script = $"({jsCode}).call(obj)";
            }
            else
            {
                // Expression — wrap in a function with 'this' bound
                script = $"(function() {{ return {jsCode}; }}).call(obj)";
            }
        }
        else
        {
            return false;
        }

        var result = engine.Evaluate(script);
        return result.AsBoolean();
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/function/
    //   "The body contains a JavaScript function that takes the args as parameters."
    private static BsonValue EvaluateFunction(string body, BsonArray args)
    {
        var engine = CreateEngine();

        // Build argument values
        var jsArgs = new JsValue[args.Count];
        for (int i = 0; i < args.Count; i++)
        {
            jsArgs[i] = BsonToJsValue(engine, args[i]);
        }

        // Evaluate: wrap body as function and call with args
        var fn = engine.Evaluate($"({body})");
        var result = engine.Invoke(fn, jsArgs);

        return JsValueToBson(result);
    }

    // Ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/accumulator/
    //   "The $accumulator defines init, accumulate, merge, and finalize functions."
    private static BsonValue EvaluateAccumulator(BsonDocument accSpec, List<BsonDocument> groupDocs, BsonDocument? variables)
    {
        var spec = accSpec["$accumulator"].AsBsonDocument;
        var initBody = spec["init"].AsString;
        var accumulateBody = spec["accumulate"].AsString;
        var mergeBody = spec.Contains("merge") ? spec["merge"].AsString : null;
        var finalizeBody = spec.Contains("finalize") ? spec["finalize"].AsString : null;
        var initArgs = spec.Contains("initArgs") ? spec["initArgs"].AsBsonArray : null;
        var accumulateArgs = spec.Contains("accumulateArgs") ? spec["accumulateArgs"].AsBsonArray : null;

        var engine = CreateEngine();

        // Initialize state
        JsValue state;
        if (initArgs != null && initArgs.Count > 0)
        {
            var jsInitArgs = initArgs.Select(a => BsonToJsValue(engine, AggregationExpressionEvaluator.Evaluate(new BsonDocument(), a, variables))).ToArray();
            var initFn = engine.Evaluate($"({initBody})");
            state = engine.Invoke(initFn, jsInitArgs);
        }
        else
        {
            state = engine.Evaluate($"({initBody})()");
        }

        // Accumulate each document
        var accFn = engine.Evaluate($"({accumulateBody})");
        foreach (var doc in groupDocs)
        {
            var docArgs = new List<JsValue> { state };
            if (accumulateArgs != null)
            {
                foreach (var argExpr in accumulateArgs)
                {
                    var val = AggregationExpressionEvaluator.Evaluate(doc, argExpr, variables);
                    docArgs.Add(BsonToJsValue(engine, val));
                }
            }
            state = engine.Invoke(accFn, docArgs.ToArray());
        }

        // Finalize
        if (finalizeBody != null)
        {
            var finFn = engine.Evaluate($"({finalizeBody})");
            state = engine.Invoke(finFn, new[] { state });
        }

        return JsValueToBson(state);
    }

    private static Engine CreateEngine()
    {
        return new Engine(options => options
            .LimitRecursion(100)
            .TimeoutInterval(TimeSpan.FromSeconds(5))
            .MaxStatements(10_000));
    }

    private static JsValue BsonToJsValue(Engine engine, BsonValue value)
    {
        return value.BsonType switch
        {
            BsonType.Null => JsValue.Null,
            BsonType.Boolean => value.AsBoolean ? JsBoolean.True : JsBoolean.False,
            BsonType.Int32 => new JsNumber(value.AsInt32),
            BsonType.Int64 => new JsNumber(value.AsInt64),
            BsonType.Double => new JsNumber(value.AsDouble),
            BsonType.String => new JsString(value.AsString),
            BsonType.Document => BsonDocumentToJsObject(engine, value.AsBsonDocument),
            BsonType.Array => BsonArrayToJsArray(engine, value.AsBsonArray),
            BsonType.ObjectId => new JsString(value.AsObjectId.ToString()),
            BsonType.DateTime => new JsNumber(value.AsBsonDateTime.MillisecondsSinceEpoch),
            _ => new JsString(value.ToString()!)
        };
    }

    private static JsValue BsonDocumentToJsObject(Engine engine, BsonDocument doc)
    {
        var obj = engine.Intrinsics.Object.Construct(Array.Empty<JsValue>());
        foreach (var element in doc)
        {
            obj.Set(element.Name, BsonToJsValue(engine, element.Value));
        }
        return obj;
    }

    private static JsValue BsonArrayToJsArray(Engine engine, BsonArray arr)
    {
        var jsArr = engine.Intrinsics.Array.Construct(Array.Empty<JsValue>());
        for (int i = 0; i < arr.Count; i++)
        {
            jsArr.Set((uint)i, BsonToJsValue(engine, arr[i]));
        }
        engine.Intrinsics.Array.PrototypeObject.Push(jsArr, Array.Empty<JsValue>());
        // Set length properly
        jsArr.Set("length", new JsNumber(arr.Count));
        return jsArr;
    }

    private static BsonValue JsValueToBson(JsValue value)
    {
        if (value.IsNull() || value.IsUndefined())
            return BsonNull.Value;
        if (value.IsBoolean())
            return new BsonBoolean(value.AsBoolean());
        if (value.IsNumber())
        {
            var num = value.AsNumber();
            if (num == Math.Floor(num) && num >= int.MinValue && num <= int.MaxValue)
                return new BsonInt32((int)num);
            return new BsonDouble(num);
        }
        if (value.IsString())
            return new BsonString(value.AsString());
        if (value.IsArray())
        {
            var arr = value.AsArray();
            var bsonArr = new BsonArray();
            foreach (var item in arr)
            {
                bsonArr.Add(JsValueToBson(item));
            }
            return bsonArr;
        }
        if (value.IsObject())
        {
            var obj = value.AsObject();
            var doc = new BsonDocument();
            foreach (var prop in obj.GetOwnProperties())
            {
                if (prop.Key.IsSymbol()) continue;
                doc[prop.Key.AsString()] = JsValueToBson(prop.Value.Value);
            }
            return doc;
        }
        return new BsonString(value.ToString());
    }
}
