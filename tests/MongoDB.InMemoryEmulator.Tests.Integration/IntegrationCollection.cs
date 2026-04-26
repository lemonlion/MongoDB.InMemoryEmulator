using Xunit;
using MongoDB.InMemoryEmulator.Tests.Infrastructure;

namespace MongoDB.InMemoryEmulator.Tests.Integration;

/// <summary>
/// xUnit collection definition. Must be in the same assembly as the tests that reference it.
/// </summary>
[CollectionDefinition(IntegrationCollection.Name)]
public class IntegrationCollection : ICollectionFixture<MongoDbSession>
{
    public const string Name = "Integration";
}
