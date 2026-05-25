using Xunit;
using InMemoryEmulator.MongoDB.Tests.Infrastructure;

namespace InMemoryEmulator.MongoDB.Tests.Integration;

/// <summary>
/// xUnit collection definition. Must be in the same assembly as the tests that reference it.
/// </summary>
[CollectionDefinition(IntegrationCollection.Name)]
public class IntegrationCollection : ICollectionFixture<MongoDbSession>
{
    public const string Name = "Integration";
}
