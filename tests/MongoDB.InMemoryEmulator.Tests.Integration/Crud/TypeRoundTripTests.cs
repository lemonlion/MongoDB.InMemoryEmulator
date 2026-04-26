using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using Xunit;
using AwesomeAssertions;

namespace MongoDB.InMemoryEmulator.Tests.Integration.Crud;

/// <summary>
/// Phase 1 integration tests: Type round-trip and serialization.
/// </summary>
[Collection("Integration")]
public class TypeRoundTripTests : IAsyncLifetime
{
    private readonly MongoDbSession _session;
    private ITestCollectionFixture _fixture = null!;

    public TypeRoundTripTests(MongoDbSession session)
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

    [Fact]
    public async Task Poco_round_trip_preserves_all_fields()
    {
        var collection = _fixture.GetCollection<TestDoc>("type_poco");
        var doc = new TestDoc
        {
            Name = "RoundTrip",
            Value = 42,
            IsActive = true,
            Tags = ["a", "b"],
            Nested = new NestedDoc { Description = "desc", Score = 3.14 }
        };

        await collection.InsertOneAsync(doc);

        var cursor = await collection.FindAsync(Builders<TestDoc>.Filter.Eq(d => d.Id, doc.Id));
        var found = await cursor.FirstOrDefaultAsync();

        found.Should().NotBeNull();
        found!.Name.Should().Be("RoundTrip");
        found.Value.Should().Be(42);
        found.IsActive.Should().BeTrue();
        found.Tags.Should().BeEquivalentTo(["a", "b"]);
        found.Nested.Should().NotBeNull();
        found.Nested!.Description.Should().Be("desc");
        found.Nested.Score.Should().Be(3.14);
    }

    [Fact]
    public async Task BsonDocument_collection_insert_and_find()
    {
        var collection = _fixture.GetCollection<BsonDocument>("type_bson");
        var doc = new BsonDocument
        {
            { "name", "BsonDoc" },
            { "value", 99 },
            { "nested", new BsonDocument("key", "val") }
        };

        await collection.InsertOneAsync(doc);

        var cursor = await collection.FindAsync(FilterDefinition<BsonDocument>.Empty);
        var results = await cursor.ToListAsync();

        results.Should().HaveCount(1);
        results[0]["name"].AsString.Should().Be("BsonDoc");
        results[0]["value"].AsInt32.Should().Be(99);
        results[0]["nested"]["key"].AsString.Should().Be("val");
    }

    [Fact]
    public async Task Shared_backing_store_BsonDocument_and_typed_views()
    {
        // Insert via typed collection
        var typed = _fixture.GetCollection<TestDoc>("shared_store");
        await typed.InsertOneAsync(new TestDoc { Name = "Shared", Value = 7 });

        // Read via BsonDocument collection
        var raw = _fixture.GetCollection<BsonDocument>("shared_store");
        var cursor = await raw.FindAsync(FilterDefinition<BsonDocument>.Empty);
        var results = await cursor.ToListAsync();

        results.Should().HaveCount(1);
        results[0]["Name"].AsString.Should().Be("Shared");
        results[0]["Value"].AsInt32.Should().Be(7);
    }

    [Fact]
    public async Task Int_string_bool_double_datetime_round_trip()
    {
        var collection = _fixture.GetCollection<BsonDocument>("type_primitives");
        var now = DateTime.UtcNow;
        var doc = new BsonDocument
        {
            { "int", 42 },
            { "str", "hello" },
            { "bool", true },
            { "double", 3.14 },
            { "date", now }
        };

        await collection.InsertOneAsync(doc);

        var cursor = await collection.FindAsync(FilterDefinition<BsonDocument>.Empty);
        var found = (await cursor.ToListAsync())[0];

        found["int"].AsInt32.Should().Be(42);
        found["str"].AsString.Should().Be("hello");
        found["bool"].AsBoolean.Should().BeTrue();
        found["double"].AsDouble.Should().Be(3.14);
    }

    [Fact]
    public async Task OrderDoc_type_round_trip()
    {
        var collection = _fixture.GetCollection<OrderDoc>("type_order");
        var order = new OrderDoc
        {
            CustomerId = "cust1",
            Total = 99.99m,
            Quantity = 3,
            Status = "pending",
            CreatedAt = new DateTime(2024, 1, 15, 12, 0, 0, DateTimeKind.Utc),
            Items =
            [
                new OrderItem { ProductName = "Widget", Price = 33.33m, Quantity = 3 }
            ]
        };

        await collection.InsertOneAsync(order);

        var cursor = await collection.FindAsync(FilterDefinition<OrderDoc>.Empty);
        var found = await cursor.FirstOrDefaultAsync();

        found.Should().NotBeNull();
        found!.CustomerId.Should().Be("cust1");
        found.Quantity.Should().Be(3);
        found.Status.Should().Be("pending");
        found.Items.Should().HaveCount(1);
        found.Items[0].ProductName.Should().Be("Widget");
    }

    [Fact]
    public async Task UserDoc_with_nested_address_round_trip()
    {
        var collection = _fixture.GetCollection<UserDoc>("type_user");
        var user = new UserDoc
        {
            Username = "johndoe",
            Email = "john@example.com",
            Age = 30,
            Address = new Address
            {
                Street = "123 Main St",
                City = "Springfield",
                State = "IL",
                ZipCode = "62704"
            }
        };

        await collection.InsertOneAsync(user);

        var cursor = await collection.FindAsync(FilterDefinition<UserDoc>.Empty);
        var found = await cursor.FirstOrDefaultAsync();

        found.Should().NotBeNull();
        found!.Username.Should().Be("johndoe");
        found.Address.Should().NotBeNull();
        found.Address!.City.Should().Be("Springfield");
        found.Address.ZipCode.Should().Be("62704");
    }
}
