# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.11.0] - 2025-07-18

### Added
- README.md with quick start, feature matrix, and wiki links
- CHANGELOG.md (this file)
- NuGet publish workflow (GitHub Actions, tag-triggered, manual approval gate)
- CodeQL security analysis in CI
- Dependabot configuration for NuGet dependency updates
- `PackageReadmeFile` metadata for NuGet packages
- Additional SdkVersionDriftDetector integration tests
- Vulnerable package check (`dotnet list package --vulnerable`) in CI

## [0.10.0] - 2025-07-18

### Added
- Unique index enforcement (single-field, compound, sparse, partial filter)
- TTL index lazy eviction on read paths
- Index validation on all write paths (Insert, Replace, Update, FindOneAnd*)
- 24 new tests (16 index enforcement + 8 TTL)

## [0.9.0] - 2025-07-18

### Added
- Fault injection via `FaultInjector` delegate (simulate errors, latency)
- Operation logging with `RequestLog` and `QueryLog`
- Concurrency stress tests
- Benchmarks (throughput, latency percentiles)

## [0.8.0] - 2025-07-18

### Added
- Capped collections with `max` and `size` document limits
- Tailable cursors via `Channel<T>` cross-thread notification
- JavaScript expression support (`$function`, `$accumulator`, `$where`) via Jint
- `MongoDB.InMemoryEmulator.JsTriggers` optional package

## [0.7.0] - 2025-07-17

### Added
- GridFS file operations (`IGridFSBucket` implementation)
- Upload, download, find, rename, delete for GridFS files
- Stream-based upload/download support

## [0.6.0] - 2025-07-17

### Added
- `$text` filter operator (case-insensitive word matching)
- Text index creation and text score projection
- Atlas `$search` / `$vectorSearch` stubs (basic substring/brute-force)

## [0.5.0] - 2025-07-17

### Added
- Advanced aggregation stages (`$graphLookup`, `$bucket`, `$bucketAuto`, `$densify`, `$fill`)
- Geospatial query operators (`$geoWithin`, `$geoIntersects`, `$near`, `$nearSphere`)
- Geospatial aggregation (`$geoNear` stage)
- All remaining expression operators
- NetTopologySuite integration for geometric calculations

## [0.4.0] - 2025-07-17

### Added
- Window functions (`$setWindowFields` with `$sum`, `$avg`, `$min`, `$max`, `$rank`, `$denseRank`)
- Change streams (watch collection, database, client)
- Client sessions and multi-document transactions (snapshot isolation)
- Views (`createView` / `db.CreateCollection` with `ViewOn` + pipeline)
- Dependency injection (`UseInMemoryMongoDB`, `UseInMemoryMongoCollections`)
- Schema validation (`$jsonSchema` via `CreateCollection` validator)
- Time series collection stubs

## [0.3.0] - 2025-07-17

### Added
- Full aggregation pipeline (34 stages)
- 100+ expression operators (arithmetic, string, date, array, conditional, type, set)
- LINQ support (`AsQueryable()` with LINQ2 and LINQ3 providers)
- Pipeline-style updates (`UpdateDefinition` from aggregation pipeline)

## [0.2.0] - 2025-07-16

### Added
- All filter operators (comparison, logical, element, array, evaluation, bitwise)
- All update operators (`$set`, `$unset`, `$inc`, `$push`, `$pull`, `$addToSet`, `$rename`, etc.)
- Projection operators (`$slice`, `$elemMatch`, `$meta`)
- Sort, skip, limit on find operations
- Collation support (culture-aware string comparison)
- MongoDB error codes and exception types

## [0.1.0] - 2025-07-16

### Added
- Initial project scaffold (solution, 5 projects, CI pipeline)
- `InMemoryMongoClient`, `InMemoryMongoDatabase`, `InMemoryMongoCollection<T>`
- Basic CRUD operations (InsertOne, InsertMany, Find, ReplaceOne, DeleteOne, DeleteMany)
- `CountDocuments`, `EstimatedDocumentCount`
- `BsonDocument` internal storage with `ConcurrentDictionary`
- xUnit v3 test infrastructure with `TestFixtureFactory` dual-target support
- GitHub Actions CI (in-memory + real MongoDB + weekly Atlas parity)
