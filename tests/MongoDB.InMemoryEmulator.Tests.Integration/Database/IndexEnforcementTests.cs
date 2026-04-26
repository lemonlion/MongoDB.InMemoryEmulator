using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;
using AwesomeAssertions;

namespace MongoDB.InMemoryEmulator.Tests.Integration.Database;

/// <summary>
/// Phase 10 integration tests: Unique index enforcement, sparse indexes,
/// partial filter indexes, compound unique indexes.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/core/index-unique/
///   "A unique index ensures that the indexed fields do not store duplicate values."
/// Ref: https://www.mongodb.com/docs/manual/core/index-sparse/
///   "Sparse indexes only contain entries for documents that have the indexed field."
/// Ref: https://www.mongodb.com/docs/manual/core/index-partial/
///   "Partial indexes only index the documents in a collection that meet a specified filter expression."
/// </remarks>
[Collection("Integration")]
public class IndexEnforcementTests : IAsyncLifetime
{
    private readonly MongoDbSession _session;
    private ITestCollectionFixture _fixture = null!;

    public IndexEnforcementTests(MongoDbSession session)
    {
        _session = session;
    }

    public ValueTask InitializeAsync()
    {
        _fixture = TestFixtureFactory.Create(_session);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await _fixture.DisposeAsync();
    }

    #region Unique Single-Field Index

    /// <summary>
    /// Ref: https://www.mongodb.com/docs/manual/core/index-unique/#unique-index-on-a-single-field
    ///   "A unique index on a single field ensures that no two documents have the same value for the indexed field."
    /// </summary>
    [Fact]
    public async Task Unique_single_field_prevents_duplicate_insert()
    {
        var collection = _fixture.GetCollection<BsonDocument>("idx_unique_single");

        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("email"),
                new CreateIndexOptions { Unique = true }));

        await collection.InsertOneAsync(new BsonDocument { { "email", "test@example.com" } });

        var act = () => collection.InsertOneAsync(new BsonDocument { { "email", "test@example.com" } });

        var ex = await act.Should().ThrowAsync<MongoWriteException>();
        ex.Which.WriteError.Category.Should().Be(ServerErrorCategory.DuplicateKey);
    }

    /// <summary>
    /// Ref: https://www.mongodb.com/docs/manual/core/index-unique/#unique-index-on-a-single-field
    ///   Different values should be allowed.
    /// </summary>
    [Fact]
    public async Task Unique_single_field_allows_different_values()
    {
        var collection = _fixture.GetCollection<BsonDocument>("idx_unique_diff");

        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("email"),
                new CreateIndexOptions { Unique = true }));

        await collection.InsertOneAsync(new BsonDocument { { "email", "a@example.com" } });
        await collection.InsertOneAsync(new BsonDocument { { "email", "b@example.com" } });

        var count = await collection.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);
        count.Should().Be(2);
    }

    /// <summary>
    /// Ref: https://www.mongodb.com/docs/manual/core/index-unique/#missing-document-field-in-a-unique-single-field-index
    ///   "If a document has a null or missing value for the indexed field in a unique single-field
    ///    index, the index stores a null value for that document."
    ///   "A single-field unique index can only contain one document that contains a null value."
    /// </summary>
    [Fact]
    public async Task Unique_single_field_allows_one_null_but_rejects_second()
    {
        var collection = _fixture.GetCollection<BsonDocument>("idx_unique_null");

        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("email"),
                new CreateIndexOptions { Unique = true }));

        // First doc without the field → stored as null → OK
        await collection.InsertOneAsync(new BsonDocument { { "name", "Alice" } });

        // Second doc without the field → duplicate null → should fail
        var act = () => collection.InsertOneAsync(new BsonDocument { { "name", "Bob" } });

        var ex = await act.Should().ThrowAsync<MongoWriteException>();
        ex.Which.WriteError.Category.Should().Be(ServerErrorCategory.DuplicateKey);
    }

    /// <summary>
    /// Unique index should also be enforced on UpdateOne.
    /// </summary>
    [Fact]
    public async Task Unique_single_field_enforced_on_update()
    {
        var collection = _fixture.GetCollection<BsonDocument>("idx_unique_update");

        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("email"),
                new CreateIndexOptions { Unique = true }));

        await collection.InsertOneAsync(new BsonDocument { { "name", "Alice" }, { "email", "alice@test.com" } });
        await collection.InsertOneAsync(new BsonDocument { { "name", "Bob" }, { "email", "bob@test.com" } });

        // Try to update Bob's email to Alice's
        var act = () => collection.UpdateOneAsync(
            Builders<BsonDocument>.Filter.Eq("name", "Bob"),
            Builders<BsonDocument>.Update.Set("email", "alice@test.com"));

        var ex = await act.Should().ThrowAsync<MongoWriteException>();
        ex.Which.WriteError.Category.Should().Be(ServerErrorCategory.DuplicateKey);
    }

    /// <summary>
    /// Unique index should be enforced on ReplaceOne.
    /// </summary>
    [Fact]
    public async Task Unique_single_field_enforced_on_replace()
    {
        var collection = _fixture.GetCollection<BsonDocument>("idx_unique_replace");

        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("email"),
                new CreateIndexOptions { Unique = true }));

        var aliceId = ObjectId.GenerateNewId();
        var bobId = ObjectId.GenerateNewId();
        await collection.InsertOneAsync(new BsonDocument { { "_id", aliceId }, { "name", "Alice" }, { "email", "alice@test.com" } });
        await collection.InsertOneAsync(new BsonDocument { { "_id", bobId }, { "name", "Bob" }, { "email", "bob@test.com" } });

        // Replace Bob's doc with Alice's email
        var act = () => collection.ReplaceOneAsync(
            Builders<BsonDocument>.Filter.Eq("_id", bobId),
            new BsonDocument { { "_id", bobId }, { "name", "Bob" }, { "email", "alice@test.com" } });

        var ex = await act.Should().ThrowAsync<MongoWriteException>();
        ex.Which.WriteError.Category.Should().Be(ServerErrorCategory.DuplicateKey);
    }

    /// <summary>
    /// After dropping a unique index, duplicates should be allowed again.
    /// </summary>
    [Fact]
    public async Task Drop_unique_index_allows_previously_blocked_duplicates()
    {
        var collection = _fixture.GetCollection<BsonDocument>("idx_unique_drop");

        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("email"),
                new CreateIndexOptions { Unique = true, Name = "email_unique" }));

        await collection.InsertOneAsync(new BsonDocument { { "email", "test@example.com" } });

        // While index exists, duplicate fails
        var act = () => collection.InsertOneAsync(new BsonDocument { { "email", "test@example.com" } });
        await act.Should().ThrowAsync<MongoWriteException>();

        // Drop the index
        await collection.Indexes.DropOneAsync("email_unique");

        // Now duplicate should succeed
        await collection.InsertOneAsync(new BsonDocument { { "email", "test@example.com" } });
        var count = await collection.CountDocumentsAsync(Builders<BsonDocument>.Filter.Eq("email", "test@example.com"));
        count.Should().Be(2);
    }

    #endregion

    #region Compound Unique Index

    /// <summary>
    /// Ref: https://www.mongodb.com/docs/manual/core/index-unique/#unique-compound-index
    ///   "A unique compound index enforces uniqueness on the combination of the index key values."
    /// </summary>
    [Fact]
    public async Task Compound_unique_prevents_duplicate_combination()
    {
        var collection = _fixture.GetCollection<BsonDocument>("idx_compound_unique");

        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("name").Ascending("email"),
                new CreateIndexOptions { Unique = true }));

        await collection.InsertOneAsync(new BsonDocument { { "name", "Alice" }, { "email", "alice@test.com" } });

        // Same combination → fail
        var act = () => collection.InsertOneAsync(new BsonDocument { { "name", "Alice" }, { "email", "alice@test.com" } });
        await act.Should().ThrowAsync<MongoWriteException>();
    }

    /// <summary>
    /// Compound unique allows same value in one field if the other differs.
    /// </summary>
    [Fact]
    public async Task Compound_unique_allows_partial_overlap()
    {
        var collection = _fixture.GetCollection<BsonDocument>("idx_compound_overlap");

        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("name").Ascending("email"),
                new CreateIndexOptions { Unique = true }));

        await collection.InsertOneAsync(new BsonDocument { { "name", "Alice" }, { "email", "alice@test.com" } });

        // Same name, different email → OK
        await collection.InsertOneAsync(new BsonDocument { { "name", "Alice" }, { "email", "alice2@test.com" } });

        // Different name, same email → OK
        await collection.InsertOneAsync(new BsonDocument { { "name", "Bob" }, { "email", "alice@test.com" } });

        var count = await collection.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);
        count.Should().Be(3);
    }

    #endregion

    #region Sparse Index

    /// <summary>
    /// Ref: https://www.mongodb.com/docs/manual/core/index-sparse/#sparse-and-unique-properties
    ///   "An index that is both sparse and unique prevents a collection from having documents
    ///    with duplicate values for a field but allows multiple documents that omit the key."
    /// </summary>
    [Fact]
    public async Task Sparse_unique_allows_multiple_missing_fields()
    {
        var collection = _fixture.GetCollection<BsonDocument>("idx_sparse_unique");

        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("email"),
                new CreateIndexOptions { Unique = true, Sparse = true }));

        // Multiple docs without the email field → all should succeed
        await collection.InsertOneAsync(new BsonDocument { { "name", "Alice" } });
        await collection.InsertOneAsync(new BsonDocument { { "name", "Bob" } });
        await collection.InsertOneAsync(new BsonDocument { { "name", "Charlie" } });

        var count = await collection.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);
        count.Should().Be(3);
    }

    /// <summary>
    /// Sparse + unique still prevents duplicates for documents that have the field.
    /// </summary>
    [Fact]
    public async Task Sparse_unique_still_prevents_duplicates_when_field_present()
    {
        var collection = _fixture.GetCollection<BsonDocument>("idx_sparse_dup");

        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("email"),
                new CreateIndexOptions { Unique = true, Sparse = true }));

        await collection.InsertOneAsync(new BsonDocument { { "email", "test@test.com" } });

        var act = () => collection.InsertOneAsync(new BsonDocument { { "email", "test@test.com" } });
        await act.Should().ThrowAsync<MongoWriteException>();
    }

    #endregion

    #region Partial Filter Index

    /// <summary>
    /// Ref: https://www.mongodb.com/docs/manual/core/index-partial/#partial-index-with-unique-constraint
    ///   "If you specify both the partialFilterExpression and a unique constraint, the unique
    ///    constraint only applies to the documents that meet the filter expression."
    /// </summary>
    [Fact]
    public async Task Partial_unique_only_enforces_on_matching_docs()
    {
        var collection = _fixture.GetCollection<BsonDocument>("idx_partial_unique");

        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("email"),
                new CreateIndexOptions<BsonDocument>
                {
                    Unique = true,
                    PartialFilterExpression = Builders<BsonDocument>.Filter.Eq("status", "active")
                }));

        // Both active → duplicate email → fail
        await collection.InsertOneAsync(new BsonDocument { { "email", "test@test.com" }, { "status", "active" } });
        var act = () => collection.InsertOneAsync(new BsonDocument { { "email", "test@test.com" }, { "status", "active" } });
        await act.Should().ThrowAsync<MongoWriteException>();
    }

    /// <summary>
    /// Non-matching docs are not subject to the unique constraint.
    /// </summary>
    [Fact]
    public async Task Partial_unique_allows_duplicates_for_non_matching_docs()
    {
        var collection = _fixture.GetCollection<BsonDocument>("idx_partial_nonmatch");

        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("email"),
                new CreateIndexOptions<BsonDocument>
                {
                    Unique = true,
                    PartialFilterExpression = Builders<BsonDocument>.Filter.Eq("status", "active")
                }));

        // Inactive docs with same email → should succeed
        await collection.InsertOneAsync(new BsonDocument { { "email", "test@test.com" }, { "status", "inactive" } });
        await collection.InsertOneAsync(new BsonDocument { { "email", "test@test.com" }, { "status", "inactive" } });

        var count = await collection.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);
        count.Should().Be(2);
    }

    /// <summary>
    /// Mix of matching and non-matching docs: unique enforced only among matching.
    /// </summary>
    [Fact]
    public async Task Partial_unique_mixed_matching_and_nonmatching()
    {
        var collection = _fixture.GetCollection<BsonDocument>("idx_partial_mixed");

        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("email"),
                new CreateIndexOptions<BsonDocument>
                {
                    Unique = true,
                    PartialFilterExpression = Builders<BsonDocument>.Filter.Eq("status", "active")
                }));

        // Active doc with email
        await collection.InsertOneAsync(new BsonDocument { { "email", "test@test.com" }, { "status", "active" } });

        // Inactive doc with same email → OK
        await collection.InsertOneAsync(new BsonDocument { { "email", "test@test.com" }, { "status", "inactive" } });

        // Another active doc with same email → fail
        var act = () => collection.InsertOneAsync(new BsonDocument { { "email", "test@test.com" }, { "status", "active" } });
        await act.Should().ThrowAsync<MongoWriteException>();
    }

    #endregion

    #region Index Metadata

    /// <summary>
    /// ListIndexes should return the partialFilterExpression in the index metadata.
    /// </summary>
    [Fact]
    public async Task ListIndexes_includes_partial_filter_expression()
    {
        var collection = _fixture.GetCollection<BsonDocument>("idx_list_partial");
        await collection.InsertOneAsync(new BsonDocument { { "x", 1 } });

        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("email"),
                new CreateIndexOptions<BsonDocument>
                {
                    Name = "email_partial",
                    PartialFilterExpression = Builders<BsonDocument>.Filter.Exists("email")
                }));

        var cursor = await collection.Indexes.ListAsync();
        var indexes = await cursor.ToListAsync();

        var partialIndex = indexes.First(i => i["name"].AsString == "email_partial");
        partialIndex.Contains("partialFilterExpression").Should().BeTrue();
    }

    /// <summary>
    /// ListIndexes should return the expireAfterSeconds for TTL indexes.
    /// </summary>
    [Fact]
    public async Task ListIndexes_includes_expireAfterSeconds()
    {
        var collection = _fixture.GetCollection<BsonDocument>("idx_list_ttl");
        await collection.InsertOneAsync(new BsonDocument { { "x", 1 } });

        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("createdAt"),
                new CreateIndexOptions
                {
                    Name = "ttl_created",
                    ExpireAfter = TimeSpan.FromSeconds(3600)
                }));

        var cursor = await collection.Indexes.ListAsync();
        var indexes = await cursor.ToListAsync();

        var ttlIndex = indexes.First(i => i["name"].AsString == "ttl_created");
        ttlIndex["expireAfterSeconds"].AsInt32.Should().Be(3600);
    }

    /// <summary>
    /// ListIndexes should return the sparse flag.
    /// </summary>
    [Fact]
    public async Task ListIndexes_includes_sparse_flag()
    {
        var collection = _fixture.GetCollection<BsonDocument>("idx_list_sparse");
        await collection.InsertOneAsync(new BsonDocument { { "x", 1 } });

        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("email"),
                new CreateIndexOptions
                {
                    Name = "email_sparse",
                    Sparse = true
                }));

        var cursor = await collection.Indexes.ListAsync();
        var indexes = await cursor.ToListAsync();

        var sparseIndex = indexes.First(i => i["name"].AsString == "email_sparse");
        sparseIndex["sparse"].AsBoolean.Should().BeTrue();
    }

    #endregion

    #region CreateIndex Validates Existing Documents

    /// <summary>
    /// Ref: https://www.mongodb.com/docs/manual/core/index-unique/#restrictions
    ///   "MongoDB cannot create a unique index on the specified index field(s) if the collection
    ///    already contains data that would violate the unique constraint for the index."
    /// </summary>
    [Fact]
    public async Task CreateOne_unique_fails_if_existing_duplicates()
    {
        var collection = _fixture.GetCollection<BsonDocument>("idx_create_dup");

        await collection.InsertOneAsync(new BsonDocument { { "email", "same@test.com" } });
        await collection.InsertOneAsync(new BsonDocument { { "email", "same@test.com" } });

        var act = () => collection.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("email"),
                new CreateIndexOptions { Unique = true }));

        await act.Should().ThrowAsync<MongoCommandException>();
    }

    #endregion
}
