using Microsoft.Extensions.DependencyInjection;
using MongoDB.Driver;

namespace MongoDB.InMemoryEmulator;

/// <summary>
/// DI registration extensions for replacing MongoDB services with in-memory implementations.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/drivers/csharp/current/fundamentals/connection/
///   "IMongoClient is the entry point for all MongoDB operations."
///
/// Usage in test setup:
///   services.UseInMemoryMongoDB(opts => {
///       opts.DatabaseName = "testdb";
///       opts.AddCollection&lt;Order&gt;("orders");
///   });
/// </remarks>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Replaces the <see cref="IMongoClient"/> and optionally <see cref="IMongoDatabase"/>
    /// and <see cref="IMongoCollection{TDocument}"/> registrations with in-memory implementations.
    /// </summary>
    public static IServiceCollection UseInMemoryMongoDB(
        this IServiceCollection services,
        Action<InMemoryMongoOptions>? configure = null)
    {
        var options = new InMemoryMongoOptions();
        configure?.Invoke(options);

        var client = new InMemoryMongoClient();
        var db = (InMemoryMongoDatabase)client.GetDatabase(options.DatabaseName);

        // Replace IMongoClient
        RemoveExisting<IMongoClient>(services);
        services.AddSingleton<IMongoClient>(client);

        // Replace IMongoDatabase
        RemoveExisting<IMongoDatabase>(services);
        services.AddSingleton<IMongoDatabase>(db);

        // Register each configured collection
        foreach (var reg in options.CollectionRegistrations)
        {
            reg(services, db);
        }

        options.OnClientCreated?.Invoke(client);
        options.OnDatabaseCreated?.Invoke(db);

        return services;
    }

    /// <summary>
    /// Replaces only <see cref="IMongoCollection{TDocument}"/> registrations. Does not replace
    /// <see cref="IMongoClient"/> or <see cref="IMongoDatabase"/>.
    /// </summary>
    public static IServiceCollection UseInMemoryMongoCollections(
        this IServiceCollection services,
        Action<InMemoryCollectionOptions>? configure = null)
    {
        var options = new InMemoryCollectionOptions();
        configure?.Invoke(options);

        var client = new InMemoryMongoClient();
        var db = (InMemoryMongoDatabase)client.GetDatabase(options.DatabaseName);

        foreach (var reg in options.CollectionRegistrations)
        {
            reg(services, db);
        }

        return services;
    }

    private static void RemoveExisting<T>(IServiceCollection services)
    {
        var existing = services.Where(s => s.ServiceType == typeof(T)).ToList();
        foreach (var descriptor in existing)
            services.Remove(descriptor);
    }
}

/// <summary>
/// Options for <see cref="ServiceCollectionExtensions.UseInMemoryMongoDB"/>.
/// </summary>
public class InMemoryMongoOptions
{
    internal List<Action<IServiceCollection, InMemoryMongoDatabase>> CollectionRegistrations { get; } = new();

    /// <summary>
    /// The database name to use. Defaults to "test".
    /// </summary>
    public string DatabaseName { get; set; } = "test";

    /// <summary>
    /// Callback invoked after the in-memory client is created.
    /// Useful for seeding data.
    /// </summary>
    public Action<InMemoryMongoClient>? OnClientCreated { get; set; }

    /// <summary>
    /// Callback invoked after the in-memory database is created.
    /// </summary>
    public Action<InMemoryMongoDatabase>? OnDatabaseCreated { get; set; }

    /// <summary>
    /// Register a collection to be available via DI.
    /// </summary>
    public InMemoryMongoOptions AddCollection<TDocument>(string collectionName)
    {
        CollectionRegistrations.Add((services, db) =>
        {
            RemoveExisting<IMongoCollection<TDocument>>(services);
            services.AddSingleton<IMongoCollection<TDocument>>(db.GetCollection<TDocument>(collectionName));
        });
        return this;
    }

    private static void RemoveExisting<T>(IServiceCollection services)
    {
        var existing = services.Where(s => s.ServiceType == typeof(T)).ToList();
        foreach (var descriptor in existing)
            services.Remove(descriptor);
    }
}

/// <summary>
/// Options for <see cref="ServiceCollectionExtensions.UseInMemoryMongoCollections"/>.
/// </summary>
public class InMemoryCollectionOptions
{
    internal List<Action<IServiceCollection, InMemoryMongoDatabase>> CollectionRegistrations { get; } = new();

    /// <summary>
    /// The database name to use. Defaults to "test".
    /// </summary>
    public string DatabaseName { get; set; } = "test";

    /// <summary>
    /// Register a collection to be available via DI.
    /// </summary>
    public InMemoryCollectionOptions AddCollection<TDocument>(string collectionName)
    {
        CollectionRegistrations.Add((services, db) =>
        {
            var existing = services.Where(s => s.ServiceType == typeof(IMongoCollection<TDocument>)).ToList();
            foreach (var descriptor in existing)
                services.Remove(descriptor);
            services.AddSingleton<IMongoCollection<TDocument>>(db.GetCollection<TDocument>(collectionName));
        });
        return this;
    }
}
