# MongoDB.InMemoryEmulator

[![NuGet](https://img.shields.io/nuget/v/MongoDB.InMemoryEmulator.svg)](https://www.nuget.org/packages/MongoDB.InMemoryEmulator/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A high-fidelity, in-process fake for the **MongoDB .NET/C# Driver** — zero Docker, zero network, instant startup.

## Why?

- **Instant** — ~0ms startup (vs 2-15s for Docker/Mongo2Go)
- **Portable** — No Docker, no `mongod` binaries, no ports
- **Reliable** — No flaky timeouts, port conflicts, or container failures
- **High-fidelity** — Real filter, update, and aggregation semantics (not mocks)

## Quick Start

```
dotnet add package MongoDB.InMemoryEmulator
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

## Optional Packages

| Package | Purpose |
|---------|---------|
| `MongoDB.InMemoryEmulator.JsTriggers` | JavaScript `$function`, `$accumulator`, `$where` via Jint |

## Documentation

See the [Wiki](https://github.com/lemonlion/MongoDB.InMemoryEmulator/wiki) for full documentation:

- [Getting Started](https://github.com/lemonlion/MongoDB.InMemoryEmulator/wiki/Getting-Started)
- [Setup Guide](https://github.com/lemonlion/MongoDB.InMemoryEmulator/wiki/Setup-Guide) (5 DI patterns)
- [Features](https://github.com/lemonlion/MongoDB.InMemoryEmulator/wiki/Features)
- [Filter & Update Operators](https://github.com/lemonlion/MongoDB.InMemoryEmulator/wiki/Filter-Update-Operators)
- [Aggregation Pipeline](https://github.com/lemonlion/MongoDB.InMemoryEmulator/wiki/Aggregation-Pipeline) (34 stages)
- [Migration Guide](https://github.com/lemonlion/MongoDB.InMemoryEmulator/wiki/Migration-Guide) (from Mongo2Go / Testcontainers)
- [Known Limitations](https://github.com/lemonlion/MongoDB.InMemoryEmulator/wiki/Known-Limitations)

## Requirements

- .NET 8.0+
- MongoDB.Driver 2.28.0+

## License

[MIT](LICENSE)
