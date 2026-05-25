using System.Reflection;
using global::MongoDB.Driver;
using Xunit;

namespace InMemoryEmulator.MongoDB.Tests.Integration;

public class SdkVersionDriftDetectorTests
{
    [Fact]
    public void Check_returns_null_for_current_driver_version()
    {
        var result = SdkVersionDriftDetector.Check();
        Assert.Null(result);
    }

    [Fact]
    public void WarnIfDrift_does_not_throw_for_current_version()
    {
        var ex = Record.Exception(() => SdkVersionDriftDetector.WarnIfDrift());
        Assert.Null(ex);
    }

    [Fact]
    public void WarnIfDrift_writes_nothing_to_stderr_for_compatible_version()
    {
        var originalErr = Console.Error;
        try
        {
            using var sw = new StringWriter();
            Console.SetError(sw);
            SdkVersionDriftDetector.WarnIfDrift();
            Assert.Empty(sw.ToString());
        }
        finally
        {
            Console.SetError(originalErr);
        }
    }

    [Fact]
    public void Current_driver_version_is_within_tested_range()
    {
        var driverAssembly = typeof(IMongoClient).Assembly;
        var version = driverAssembly.GetName().Version;
        Assert.NotNull(version);
        Assert.True(version >= new Version(2, 0, 0, 0),
            $"Driver version {version} is below minimum 2.0.0.0");
        Assert.True(version <= new Version(2, 30, 0, 0),
            $"Driver version {version} is above maximum 2.30.0.0");
    }

    [Fact]
    public void Check_resolves_driver_assembly_version_successfully()
    {
        var driverAssembly = typeof(IMongoClient).Assembly;
        var version = driverAssembly.GetName().Version;
        Assert.NotNull(version);
        Assert.True(version.Major >= 2, $"Expected major version >= 2, got {version.Major}");
    }
}
