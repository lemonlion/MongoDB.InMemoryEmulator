# <img src="icon.svg" width="32" height="32" alt="InMemoryEmulator.MongoDB icon" style="vertical-align: middle;"> InMemoryEmulator.MongoDB

[![NuGet](https://img.shields.io/nuget/v/InMemoryEmulator.MongoDB.svg)](https://www.nuget.org/packages/InMemoryEmulator.MongoDB/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A high-fidelity, in-process fake for the **MongoDB .NET/C# Driver** — zero Docker, zero network, instant startup.

## Why?

- **Instant** — ~0ms startup (vs 2-15s for Docker/Mongo2Go)
- **Portable** — No Docker, no `mongod` binaries, no ports
- **Reliable** — No flaky timeouts, port conflicts, or container failures
- **High-fidelity** — Real filter, update, and aggregation semantics (not mocks)

## Quick Start

```
dotnet add package InMemoryEmulator.MongoDB          # for MongoDB.Driver 3.x
dotnet add package InMemoryEmulator.MongoDB.V2Driver  # for MongoDB.Driver 2.x
```

### Unit Tests

```csharp
var result = InMemoryMongo.Create<Order>("orders");
var collection = result.Collection;

await collection.InsertOneAsync(new Order { Total = 99.99m });
var found = await collection.Find(o => o.Total > 50).FirstOrDefaultAsync();
```

### Integration Tests (DI)

```csharp
services.UseInMemoryMongoDB(options =>
{
    options.DatabaseName = "testdb";
    options.AddCollection<Order>("orders");
});
```

## What's Supported

| Feature | Status |
|---------|--------|
| Full CRUD (Insert, Find, Update, Replace, Delete, BulkWrite) | ✅ |
| Aggregation Pipeline (34 stages, 100+ expression operators) | ✅ |
| Indexes (Unique, Compound, Sparse, Partial Filter, TTL) | ✅ |
| Change Streams | ✅ |
| Transactions (snapshot isolation) | ✅ |
| GridFS | ✅ |
| Schema Validation (`$jsonSchema`) | ✅ |
| Capped Collections + Tailable Cursors | ✅ |
| Views | ✅ |
| LINQ (`AsQueryable()`) | ✅ |
| Fault Injection | ✅ |
| State Persistence (Export/Import) | ✅ |
| DI Integration (2 extension methods) | ✅ |

## NuGet Packages

| Package | Description | NuGet |
|---|---|---|
| `InMemoryEmulator.MongoDB` | Core library for **MongoDB.Driver 3.x** | [![NuGet Version](https://img.shields.io/nuget/v/InMemoryEmulator.MongoDB)](https://www.nuget.org/packages/InMemoryEmulator.MongoDB) |
| `InMemoryEmulator.MongoDB.V2Driver` | Core library for **MongoDB.Driver 2.x** | [![NuGet Version](https://img.shields.io/nuget/v/InMemoryEmulator.MongoDB.V2Driver)](https://www.nuget.org/packages/InMemoryEmulator.MongoDB.V2Driver) |
| `InMemoryEmulator.MongoDB.JsTriggers` | `$function`, `$accumulator`, `$where` via Jint | [![NuGet Version](https://img.shields.io/nuget/v/InMemoryEmulator.MongoDB.JsTriggers)](https://www.nuget.org/packages/InMemoryEmulator.MongoDB.JsTriggers) |

> Both packages share the same namespace (`InMemoryEmulator.MongoDB`) and feature set. Pick the one matching your driver version — the API is identical.

## Documentation

See the [Wiki](https://github.com/lemonlion/InMemoryEmulator.MongoDB/wiki) for full documentation:

- [Getting Started](https://github.com/lemonlion/InMemoryEmulator.MongoDB/wiki/Getting-Started)
- [Setup Guide](https://github.com/lemonlion/InMemoryEmulator.MongoDB/wiki/Setup-Guide) (5 DI patterns)
- [Features](https://github.com/lemonlion/InMemoryEmulator.MongoDB/wiki/Features)
- [Filter & Update Operators](https://github.com/lemonlion/InMemoryEmulator.MongoDB/wiki/Filter-Update-Operators)
- [Aggregation Pipeline](https://github.com/lemonlion/InMemoryEmulator.MongoDB/wiki/Aggregation-Pipeline) (34 stages)
- [Migration Guide](https://github.com/lemonlion/InMemoryEmulator.MongoDB/wiki/Migration-Guide) (from Mongo2Go / Testcontainers)
- [Known Limitations](https://github.com/lemonlion/InMemoryEmulator.MongoDB/wiki/Known-Limitations)

## Requirements

- .NET 8.0+
- `InMemoryEmulator.MongoDB` requires MongoDB.Driver **3.x** (tested up to 3.8.1)
- `InMemoryEmulator.MongoDB.V2Driver` requires MongoDB.Driver **2.x** (tested up to 2.30.0)

## License

[MIT](LICENSE)
