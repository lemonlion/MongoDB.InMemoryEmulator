using Microsoft.Extensions.DependencyInjection;
using MongoDB.Bson;
using MongoDB.Driver;
using Xunit;
using AwesomeAssertions;

namespace MongoDB.InMemoryEmulator.Tests.Integration;

/// <summary>
/// Phase 4 integration tests: DI registration via UseInMemoryMongoDB.
/// </summary>
public class DependencyInjectionTests
{
    [Fact]
    public void UseInMemoryMongoDB_registers_client_and_database()
    {
        // Ref: https://www.mongodb.com/docs/drivers/csharp/current/fundamentals/connection/
        //   "IMongoClient is the entry point for all MongoDB operations."
        var services = new ServiceCollection();
        services.UseInMemoryMongoDB(opts =>
        {
            opts.DatabaseName = "di_test";
        });

        var provider = services.BuildServiceProvider();

        var client = provider.GetRequiredService<IMongoClient>();
        var db = provider.GetRequiredService<IMongoDatabase>();

        Assert.NotNull(client);
        Assert.IsType<InMemoryMongoClient>(client);
        Assert.NotNull(db);
        Assert.IsType<InMemoryMongoDatabase>(db);
        Assert.Equal("di_test", db.DatabaseNamespace.DatabaseName);
    }

    [Fact]
    public void UseInMemoryMongoDB_registers_collections()
    {
        var services = new ServiceCollection();
        services.UseInMemoryMongoDB(opts =>
        {
            opts.DatabaseName = "di_coll_test";
            opts.AddCollection<BsonDocument>("items");
            opts.AddCollection<BsonDocument>("users");
        });

        var provider = services.BuildServiceProvider();

        var collection = provider.GetRequiredService<IMongoCollection<BsonDocument>>();
        Assert.NotNull(collection);
    }

    [Fact]
    public void UseInMemoryMongoDB_replaces_existing_registrations()
    {
        var services = new ServiceCollection();
        // First add a "real" registration
        services.AddSingleton<IMongoClient>(new MongoClient("mongodb://fake:27017"));

        // Then replace with in-memory
        services.UseInMemoryMongoDB();

        var provider = services.BuildServiceProvider();
        var client = provider.GetRequiredService<IMongoClient>();
        Assert.IsType<InMemoryMongoClient>(client);
    }

    [Fact]
    public async Task UseInMemoryMongoDB_OnClientCreated_callback()
    {
        InMemoryMongoClient? capturedClient = null;

        var services = new ServiceCollection();
        services.UseInMemoryMongoDB(opts =>
        {
            opts.DatabaseName = "callback_test";
            opts.OnClientCreated = client => capturedClient = client;
        });

        var provider = services.BuildServiceProvider();
        Assert.NotNull(capturedClient);
    }

    [Fact]
    public async Task UseInMemoryMongoDB_seeding_via_OnDatabaseCreated()
    {
        var services = new ServiceCollection();
        services.UseInMemoryMongoDB(opts =>
        {
            opts.DatabaseName = "seed_test";
            opts.AddCollection<BsonDocument>("items");
            opts.OnDatabaseCreated = db =>
            {
                var coll = db.GetCollection<BsonDocument>("items");
                coll.InsertOne(new BsonDocument { { "_id", "seed1" }, { "val", 99 } });
            };
        });

        var provider = services.BuildServiceProvider();
        var collection = provider.GetRequiredService<IMongoCollection<BsonDocument>>();

        var cursor = await collection.FindAsync(FilterDefinition<BsonDocument>.Empty);
        var results = await cursor.ToListAsync();
        Assert.Single(results);
        Assert.Equal(99, results[0]["val"].AsInt32);
    }

    [Fact]
    public void UseInMemoryMongoCollections_only_replaces_collections()
    {
        var services = new ServiceCollection();

        // Should not register IMongoClient or IMongoDatabase
        services.UseInMemoryMongoCollections(opts =>
        {
            opts.DatabaseName = "coll_only";
            opts.AddCollection<BsonDocument>("orders");
        });

        var provider = services.BuildServiceProvider();

        // Client and database should NOT be registered
        var client = provider.GetService<IMongoClient>();
        Assert.Null(client);

        var db = provider.GetService<IMongoDatabase>();
        Assert.Null(db);

        // Collection SHOULD be registered
        var coll = provider.GetService<IMongoCollection<BsonDocument>>();
        Assert.NotNull(coll);
    }

    [Fact]
    public void UseInMemoryMongoDB_default_database_name_is_test()
    {
        var services = new ServiceCollection();
        services.UseInMemoryMongoDB();

        var provider = services.BuildServiceProvider();
        var db = provider.GetRequiredService<IMongoDatabase>();
        Assert.Equal("test", db.DatabaseNamespace.DatabaseName);
    }
}
