using System;
using System.Reflection;
using System.Linq;
using MongoDB.Driver;
var t = typeof(BulkWriteUpsert);
foreach(var c in t.GetConstructors(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance))
    Console.WriteLine(string.Join("", "", c.GetParameters().Select(p => $""{p.ParameterType.Name} {p.Name}"")));
var t2 = typeof(BulkWriteResult<>);
foreach(var m in t2.GetNestedTypes()) Console.WriteLine(m.Name);
