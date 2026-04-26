using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;
using AwesomeAssertions;

namespace MongoDB.InMemoryEmulator.Tests.Integration.Database;

/// <summary>
/// Phase 10 integration tests: TTL index enforcement with lazy eviction.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/manual/core/index-ttl/
///   "TTL indexes are special single-field indexes that MongoDB can use to automatically
///    remove documents from a collection after a certain amount of time."
///   "If the indexed field in a document doesn't contain one or more date values,
///    the document will not expire."
///   "If a document does not contain the indexed field, the document will not expire."
/// TTL eviction in the in-memory emulator is lazy (on read), unlike real MongoDB which uses a
/// background thread (60-second interval). These tests verify lazy eviction semantics.
/// </remarks>
[Collection("Integration")]
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class TtlIndexTests : IAsyncLifetime
{
    private readonly MongoDbSession _session;
    private ITestCollectionFixture _fixture = null!;

    public TtlIndexTests(MongoDbSession session)
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

    /// <summary>
    /// Expired documents should not be returned by Find queries (lazy eviction).
    /// Ref: https://www.mongodb.com/docs/manual/core/index-ttl/#expiration-of-data
    ///   "TTL indexes expire documents after the specified number of seconds has passed
    ///    since the indexed field value."
    /// </summary>
    [Fact]
    public async Task Expired_documents_not_returned_by_find()
    {
        var collection = _fixture.GetCollection<BsonDocument>("ttl_expired");

        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("expiresAt"),
                new CreateIndexOptions { ExpireAfter = TimeSpan.FromSeconds(60) }));

        // Insert a document that expired 10 minutes ago
        var expiredDate = DateTime.UtcNow.AddMinutes(-10);
        await collection.InsertOneAsync(new BsonDocument { { "name", "expired" }, { "expiresAt", expiredDate } });

        // Insert a document that won't expire for an hour
        var futureDate = DateTime.UtcNow.AddHours(1);
        await collection.InsertOneAsync(new BsonDocument { { "name", "valid" }, { "expiresAt", futureDate } });

        var results = await collection.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();
        results.Should().ContainSingle();
        results[0]["name"].AsString.Should().Be("valid");
    }

    /// <summary>
    /// Documents without the TTL-indexed field should never expire.
    /// Ref: https://www.mongodb.com/docs/manual/core/index-ttl/#expiration-of-data
    ///   "If a document does not contain the indexed field, the document will not expire."
    /// </summary>
    [Fact]
    public async Task Documents_without_ttl_field_do_not_expire()
    {
        var collection = _fixture.GetCollection<BsonDocument>("ttl_no_field");

        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("expiresAt"),
                new CreateIndexOptions { ExpireAfter = TimeSpan.FromSeconds(1) }));

        // Insert doc without the expiresAt field
        await collection.InsertOneAsync(new BsonDocument { { "name", "persistent" } });

        var results = await collection.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();
        results.Should().ContainSingle();
        results[0]["name"].AsString.Should().Be("persistent");
    }

    /// <summary>
    /// Documents with a non-date value in the TTL field should never expire.
    /// Ref: https://www.mongodb.com/docs/manual/core/index-ttl/#expiration-of-data
    ///   "If the indexed field in a document doesn't contain one or more date values,
    ///    the document will not expire."
    /// </summary>
    [Fact]
    public async Task Documents_with_non_date_ttl_field_do_not_expire()
    {
        var collection = _fixture.GetCollection<BsonDocument>("ttl_non_date");

        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("expiresAt"),
                new CreateIndexOptions { ExpireAfter = TimeSpan.FromSeconds(1) }));

        // Insert doc with a string in the TTL field
        await collection.InsertOneAsync(new BsonDocument { { "name", "persistent" }, { "expiresAt", "not-a-date" } });

        var results = await collection.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();
        results.Should().ContainSingle();
    }

    /// <summary>
    /// CountDocuments should exclude TTL-expired documents.
    /// </summary>
    [Fact]
    public async Task CountDocuments_excludes_expired()
    {
        var collection = _fixture.GetCollection<BsonDocument>("ttl_count");

        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("expiresAt"),
                new CreateIndexOptions { ExpireAfter = TimeSpan.FromSeconds(60) }));

        // 2 expired, 1 valid
        await collection.InsertOneAsync(new BsonDocument { { "name", "e1" }, { "expiresAt", DateTime.UtcNow.AddMinutes(-10) } });
        await collection.InsertOneAsync(new BsonDocument { { "name", "e2" }, { "expiresAt", DateTime.UtcNow.AddMinutes(-5) } });
        await collection.InsertOneAsync(new BsonDocument { { "name", "valid" }, { "expiresAt", DateTime.UtcNow.AddHours(1) } });

        var count = await collection.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);
        count.Should().Be(1);
    }

    /// <summary>
    /// TTL expired docs should also be evicted from the store (lazy removal).
    /// After a query triggers eviction, the document count in the store should decrease.
    /// </summary>
    [Fact]
    public async Task Ttl_eviction_removes_expired_from_store()
    {
        var collection = _fixture.GetCollection<BsonDocument>("ttl_evict");

        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("expiresAt"),
                new CreateIndexOptions { ExpireAfter = TimeSpan.FromSeconds(60) }));

        var expiredDate = DateTime.UtcNow.AddMinutes(-10);
        await collection.InsertOneAsync(new BsonDocument { { "name", "expired" }, { "expiresAt", expiredDate } });
        await collection.InsertOneAsync(new BsonDocument { { "name", "valid" }, { "expiresAt", DateTime.UtcNow.AddHours(1) } });

        // Query to trigger eviction
        await collection.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();

        // After eviction, even a direct count should show 1
        var count = await collection.CountDocumentsAsync(FilterDefinition<BsonDocument>.Empty);
        count.Should().Be(1);
    }

    /// <summary>
    /// TTL with expireAfterSeconds = 0 means docs expire at the exact date field value.
    /// Ref: https://www.mongodb.com/docs/manual/tutorial/expire-data/#expire-documents-at-a-specific-clock-time
    ///   "To expire documents at a specific clock time, set expireAfterSeconds to 0."
    /// </summary>
    [Fact]
    public async Task Ttl_zero_seconds_expires_at_field_value()
    {
        var collection = _fixture.GetCollection<BsonDocument>("ttl_zero");

        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("expiresAt"),
                new CreateIndexOptions { ExpireAfter = TimeSpan.Zero }));

        // Past date → expired
        await collection.InsertOneAsync(new BsonDocument { { "name", "past" }, { "expiresAt", DateTime.UtcNow.AddMinutes(-1) } });

        // Future date → not expired
        await collection.InsertOneAsync(new BsonDocument { { "name", "future" }, { "expiresAt", DateTime.UtcNow.AddHours(1) } });

        var results = await collection.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();
        results.Should().ContainSingle();
        results[0]["name"].AsString.Should().Be("future");
    }

    /// <summary>
    /// TTL expiration should work with array of dates (uses lowest/earliest date).
    /// Ref: https://www.mongodb.com/docs/manual/core/index-ttl/#expiration-of-data
    ///   "If the field is an array, and there are multiple date values in the index,
    ///    MongoDB uses lowest (earliest) date value in the array to calculate the expiration threshold."
    /// </summary>
    [Fact]
    public async Task Ttl_with_array_uses_earliest_date()
    {
        var collection = _fixture.GetCollection<BsonDocument>("ttl_array");

        await collection.Indexes.CreateOneAsync(
            new CreateIndexModel<BsonDocument>(
                Builders<BsonDocument>.IndexKeys.Ascending("dates"),
                new CreateIndexOptions { ExpireAfter = TimeSpan.FromSeconds(60) }));

        // Array with one expired date → doc is expired (earliest is in the past)
        await collection.InsertOneAsync(new BsonDocument
        {
            { "name", "expired" },
            { "dates", new BsonArray { DateTime.UtcNow.AddMinutes(-10), DateTime.UtcNow.AddHours(1) } }
        });

        // Array with all future dates → doc is not expired
        await collection.InsertOneAsync(new BsonDocument
        {
            { "name", "valid" },
            { "dates", new BsonArray { DateTime.UtcNow.AddHours(1), DateTime.UtcNow.AddHours(2) } }
        });

        var results = await collection.Find(FilterDefinition<BsonDocument>.Empty).ToListAsync();
        results.Should().ContainSingle();
        results[0]["name"].AsString.Should().Be("valid");
    }
}
