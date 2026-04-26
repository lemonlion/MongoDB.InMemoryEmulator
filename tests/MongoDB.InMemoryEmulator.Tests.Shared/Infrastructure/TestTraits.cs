namespace MongoDB.InMemoryEmulator.Tests.Infrastructure;

/// <summary>
/// Trait constants for test classification.
/// Use with <c>[Trait(TestTraits.Target, TestTraits.InMemoryOnly)]</c>.
/// </summary>
public static class TestTraits
{
    public const string Target = "Target";
    public const string All = "All";
    public const string InMemoryOnly = "InMemoryOnly";
    public const string MongoDbOnly = "MongoDbOnly";
    public const string AtlasOnly = "AtlasOnly";
    public const string KnownDivergence = "KnownDivergence";
}
