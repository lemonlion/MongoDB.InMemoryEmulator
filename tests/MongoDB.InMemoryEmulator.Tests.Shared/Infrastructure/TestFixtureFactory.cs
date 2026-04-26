namespace MongoDB.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Factory for creating the appropriate test fixture based on the current test target.
/// </summary>
public static class TestFixtureFactory
{
    public static ITestCollectionFixture Create(MongoDbSession session) =>
        session.IsRealMongo
            ? new MongoDbTestFixture(session)
            : new InMemoryTestFixture();
}
