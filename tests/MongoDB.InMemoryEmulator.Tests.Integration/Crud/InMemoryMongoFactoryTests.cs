using MongoDB.Driver;
using Xunit;
using AwesomeAssertions;

namespace MongoDB.InMemoryEmulator.Tests.Integration.Crud;

/// <summary>
/// Phase 1 integration tests: InMemoryMongo static factory.
/// </summary>
[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
public class InMemoryMongoFactoryTests
{
    [Fact]
    public async Task Create_returns_working_client_database_collection()
    {
        var result = InMemoryMongo.Create<TestDoc>();

        await result.Collection.InsertOneAsync(new TestDoc { Name = "Factory" });

        var count = await result.Collection.CountDocumentsAsync(FilterDefinition<TestDoc>.Empty);
        count.Should().Be(1);
    }

    [Fact]
    public async Task Create_with_custom_names()
    {
        var result = InMemoryMongo.Create<TestDoc>("my_collection", "my_database");

        result.Database.DatabaseNamespace.DatabaseName.Should().Be("my_database");
        result.Collection.CollectionNamespace.CollectionName.Should().Be("my_collection");

        await result.Collection.InsertOneAsync(new TestDoc { Name = "Custom" });
        var count = await result.Collection.CountDocumentsAsync(FilterDefinition<TestDoc>.Empty);
        count.Should().Be(1);
    }

    [Fact]
    public void Builder_creates_configured_client()
    {
        var client = InMemoryMongo.Builder()
            .AddDatabase("custom_db", db => db.AddCollection<TestDoc>("items"))
            .Build();

        client.Should().NotBeNull();
        var db = client.GetDatabase("custom_db");
        db.DatabaseNamespace.DatabaseName.Should().Be("custom_db");
    }
}
