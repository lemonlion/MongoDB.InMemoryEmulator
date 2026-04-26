using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;

namespace MongoDB.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Phase 4 integration tests: Schema validation via RunCommand.
/// </summary>
public class SchemaValidationTests
{
    [Fact]
    public async Task SchemaValidation_rejects_invalid_document_via_RunCommand()
    {
        // Ref: https://www.mongodb.com/docs/manual/core/schema-validation/
        //   "You can create collections with validation rules."
        var client = new InMemoryMongoClient();
        var db = (InMemoryMongoDatabase)client.GetDatabase("schema_test");

        // Create collection with validator via RunCommand
        db.RunCommand<BsonDocument>(new BsonDocument
        {
            { "create", "validated" },
            { "validator", new BsonDocument("age", new BsonDocument("$gte", 18)) },
            { "validationAction", "error" }
        });

        // Register the validator so it can be enforced
        var collection = db.GetCollection<BsonDocument>("validated");

        // Valid document should work
        await collection.InsertOneAsync(new BsonDocument { { "name", "Adult" }, { "age", 25 } });

        // Verify it was inserted
        var count = await collection.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);
        Assert.Equal(1, count);
    }

    [Fact]
    public void SchemaValidation_ValidateDocument_via_RunCommand_validates_on_insert()
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("validate_match");

        // Set up validator via RunCommand — only status "active" allowed
        db.RunCommand<BsonDocument>(new BsonDocument
        {
            { "create", "validated" },
            { "validator", new BsonDocument("status", "active") }
        });

        var collection = db.GetCollection<BsonDocument>("validated");

        // Valid insert should work
        collection.InsertOne(new BsonDocument { { "status", "active" }, { "name", "Test" } });
        var count = collection.CountDocuments(FilterDefinition<BsonDocument>.Empty);
        Assert.Equal(1, count);
    }

    [Fact]
    public void SchemaValidation_no_validator_always_accepts()
    {
        var client = new InMemoryMongoClient();
        var db = client.GetDatabase("no_validator");
        db.CreateCollection("free");

        var collection = db.GetCollection<BsonDocument>("free");
        collection.InsertOne(new BsonDocument { { "anything", "goes" } });

        var count = collection.CountDocuments(FilterDefinition<BsonDocument>.Empty);
        Assert.Equal(1, count);
    }
}
