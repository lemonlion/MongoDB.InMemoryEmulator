using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoDB.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Tests for $jsonSchema filter operator.
/// </summary>
public class JsonSchemaTests
{
    private IMongoCollection<BsonDocument> CreateCollection()
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("test");
        return db.GetCollection<BsonDocument>("items");
    }

    [Fact]
    public void JsonSchema_RequiredFieldsMatch()
    {
        // Ref: https://www.mongodb.com/docs/manual/reference/operator/query/jsonSchema/
        //   "The $jsonSchema operator matches documents that satisfy the specified JSON Schema."
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "name", "Alice" }, { "age", 30 } });
        col.InsertOne(new BsonDocument { { "name", "Bob" } }); // Missing age

        var schema = new BsonDocument
        {
            { "$jsonSchema", new BsonDocument
                {
                    { "required", new BsonArray { "name", "age" } }
                }
            }
        };

        var results = col.Find(schema).ToList();
        Assert.Single(results);
        Assert.Equal("Alice", results[0]["name"].AsString);
    }

    [Fact]
    public void JsonSchema_PropertyTypeValidation()
    {
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "age", 30 } });
        col.InsertOne(new BsonDocument { { "age", "thirty" } }); // String instead of int

        var schema = new BsonDocument
        {
            { "$jsonSchema", new BsonDocument
                {
                    { "properties", new BsonDocument
                        {
                            { "age", new BsonDocument { { "bsonType", "int" } } }
                        }
                    }
                }
            }
        };

        var results = col.Find(schema).ToList();
        Assert.Single(results);
        Assert.Equal(30, results[0]["age"].AsInt32);
    }

    [Fact]
    public void JsonSchema_NumericConstraints()
    {
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "score", 50 } });
        col.InsertOne(new BsonDocument { { "score", 150 } });
        col.InsertOne(new BsonDocument { { "score", 5 } });

        var schema = new BsonDocument
        {
            { "$jsonSchema", new BsonDocument
                {
                    { "properties", new BsonDocument
                        {
                            { "score", new BsonDocument
                                {
                                    { "minimum", 10 },
                                    { "maximum", 100 }
                                }
                            }
                        }
                    }
                }
            }
        };

        var results = col.Find(schema).ToList();
        Assert.Single(results);
        Assert.Equal(50, results[0]["score"].AsInt32);
    }

    [Fact]
    public void JsonSchema_StringLengthConstraints()
    {
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "code", "AB" } });
        col.InsertOne(new BsonDocument { { "code", "ABCDE" } });
        col.InsertOne(new BsonDocument { { "code", "A" } });

        var schema = new BsonDocument
        {
            { "$jsonSchema", new BsonDocument
                {
                    { "properties", new BsonDocument
                        {
                            { "code", new BsonDocument
                                {
                                    { "bsonType", "string" },
                                    { "minLength", 2 },
                                    { "maxLength", 4 }
                                }
                            }
                        }
                    }
                }
            }
        };

        var results = col.Find(schema).ToList();
        Assert.Single(results);
        Assert.Equal("AB", results[0]["code"].AsString);
    }

    [Fact]
    public void JsonSchema_PatternConstraint()
    {
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "email", "user@example.com" } });
        col.InsertOne(new BsonDocument { { "email", "invalid" } });

        var schema = new BsonDocument
        {
            { "$jsonSchema", new BsonDocument
                {
                    { "properties", new BsonDocument
                        {
                            { "email", new BsonDocument
                                {
                                    { "bsonType", "string" },
                                    { "pattern", "@.*\\." }
                                }
                            }
                        }
                    }
                }
            }
        };

        var results = col.Find(schema).ToList();
        Assert.Single(results);
        Assert.Equal("user@example.com", results[0]["email"].AsString);
    }

    [Fact]
    public void JsonSchema_EnumValues()
    {
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "status", "active" } });
        col.InsertOne(new BsonDocument { { "status", "inactive" } });
        col.InsertOne(new BsonDocument { { "status", "unknown" } });

        var schema = new BsonDocument
        {
            { "$jsonSchema", new BsonDocument
                {
                    { "properties", new BsonDocument
                        {
                            { "status", new BsonDocument
                                {
                                    { "enum", new BsonArray { "active", "inactive" } }
                                }
                            }
                        }
                    }
                }
            }
        };

        var results = col.Find(schema).ToList();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public void JsonSchema_ArrayConstraints()
    {
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "tags", new BsonArray { "a", "b" } } });
        col.InsertOne(new BsonDocument { { "tags", new BsonArray { "a" } } });
        col.InsertOne(new BsonDocument { { "tags", new BsonArray { "a", "b", "c", "d", "e" } } });

        var schema = new BsonDocument
        {
            { "$jsonSchema", new BsonDocument
                {
                    { "properties", new BsonDocument
                        {
                            { "tags", new BsonDocument
                                {
                                    { "bsonType", "array" },
                                    { "minItems", 2 },
                                    { "maxItems", 3 }
                                }
                            }
                        }
                    }
                }
            }
        };

        var results = col.Find(schema).ToList();
        Assert.Single(results);
        Assert.Equal(2, results[0]["tags"].AsBsonArray.Count);
    }

    [Fact]
    public void JsonSchema_MultipleBsonTypes()
    {
        var col = CreateCollection();
        col.InsertOne(new BsonDocument { { "value", 42 } });
        col.InsertOne(new BsonDocument { { "value", "hello" } });
        col.InsertOne(new BsonDocument { { "value", true } });

        var schema = new BsonDocument
        {
            { "$jsonSchema", new BsonDocument
                {
                    { "properties", new BsonDocument
                        {
                            { "value", new BsonDocument
                                {
                                    { "bsonType", new BsonArray { "int", "string" } }
                                }
                            }
                        }
                    }
                }
            }
        };

        var results = col.Find(schema).ToList();
        Assert.Equal(2, results.Count);
    }
}
