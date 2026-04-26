# Contribution Instructions

## TDD Workflow

- Always use Test-Driven Development (TDD): write tests first, then follow the red-green-refactor cycle.
- Write a failing test (red), implement the minimum code to make it pass (green), then refactor.
- Write additional failing tests to cover edge cases and error conditions, and repeat the cycle until you have comprehensive test coverage for the feature or bug fix you're working on.
- For every unit test written, if possible, write the equivalent integration test, testing the same functionality from the entry point.

## Bug Fixing

- Always fix all bugs you find along the way, even if they are outside the immediate scope of the current task.
- When fixing a bug, identify missing test coverage in and around the affected area and create that coverage — again following the TDD red-green-refactor cycle.
- Fix any additional bugs discovered during that expanded test coverage work.

## Reflection Policy

- **Do not use reflection as a first resort.** Explore all public API options before considering reflection.
- Reflection on internal/private members of external libraries (e.g., SDK backing fields) is fragile — it can break silently on library updates with no compile-time warning.
- If reflection is genuinely the only viable approach after exhausting alternatives, it may be used — but:
  - **The PR description must explicitly state in bold that reflection is used**, what it targets, and why no public API alternative exists.
  - Add a code comment at the reflection site explaining the dependency and what would break if the internal member is renamed or removed.
  - Prefer a graceful fallback (e.g., leave the value as null) over a hard failure if the reflected member is missing.

## Behavioral Source Requirements

Every piece of behavioral logic in the source code — status codes, validation rules, error conditions, side-effect semantics — **must** be backed by a verified source. This prevents accidental divergence from real MongoDB behavior.

### Rules

1. **Before implementing any behavioral logic**, find and verify the expected behavior from one of the approved sources listed below.
2. **Add a code comment** at the implementation site citing the source (a short URL or description is sufficient). Example:
   ```csharp
   // Ref: https://www.mongodb.com/docs/manual/reference/command/find/
   //   "The find command returns a cursor to the documents that match the query criteria."
   ```
3. **If sources conflict** (e.g., the MongoDB Shell behaves differently from the documentation), prefer the official documentation over observed behavior. Document the discrepancy in a code comment and mark the relevant integration test with `[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]`.
4. **If no source can be found**, do not guess. Ask for guidance or raise a discussion in the PR.

### Approved Behavioral Sources (in priority order)

| Priority | Source | URL / Location |
|----------|--------|----------------|
| 1 | MongoDB Server Manual (Commands Reference) | https://www.mongodb.com/docs/manual/reference/command/ |
| 2 | MongoDB Server Manual (CRUD Operations) | https://www.mongodb.com/docs/manual/crud/ |
| 3 | MongoDB Server Manual (Aggregation) | https://www.mongodb.com/docs/manual/aggregation/ |
| 4 | MongoDB .NET/C# Driver API reference | https://www.mongodb.com/docs/drivers/csharp/current/ |
| 5 | MongoDB Server Manual (Indexes) | https://www.mongodb.com/docs/manual/indexes/ |
| 6 | MongoDB Server Manual (Conceptual docs) | https://www.mongodb.com/docs/manual/ |
| 7 | MongoDB .NET/C# Driver source code | https://github.com/mongodb/mongo-csharp-driver |
| 8 | Observed behavior on a real MongoDB instance | (local testing via `mongod` or Docker container) |

> **Note:** Source 8 (observed behavior on a real instance) is the weakest evidence. Always cross-reference with sources 1–7 when possible.

## Versioning & Release

- After every session of bug fixes is complete and the full test suite has passed, increment the patch version in `src/Directory.Build.props` (the single `<Version>` property shared by all packages).
- **On `main`:** Commit, create a git tag (`v{version}`), and push both the commit and the tag to origin.
- **On any other branch:** Commit and push the code changes and version bump only. Do not create or push a tag.

## Test Classification Rules

Tests are split into two projects. When creating or moving tests, follow these rules:

### Tests.Integration
- Uses the real MongoDB .NET Driver pipeline via the in-process test server
- Must **not** use any `internal` API directly
- Can run against in-memory or a real MongoDB instance
- **This is the primary test project** — every test should be an integration test unless it requires internal API access

### Tests.Unit
- Uses internal APIs directly
- Tests that touch internal APIs (e.g., fault injection internals) belong here
- Only runs in-memory — never against a real MongoDB instance

### Tests.Shared
- Class library (not a test project) — shared infrastructure, fixtures, traits, and models
- Referenced by both Unit and Integration projects

### Key constraint
The Integration project does **not** have `InternalsVisibleTo` access. If a test needs internal APIs, it belongs in Unit.

## Documentation

After any changes are made that might affect the public API or functionality, documentation must be updated to reflect those changes. The documentation should be clear and comprehensive, covering all new features, changes to existing features, and any deprecations or removals. This includes updating the README file (if relevant), but mainly the wiki which can be found in a sister folder to the main repository — `../MongoDB.InMemoryEmulator.wiki`.
