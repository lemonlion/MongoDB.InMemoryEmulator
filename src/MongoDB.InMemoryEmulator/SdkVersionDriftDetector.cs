using System.Reflection;

namespace MongoDB.InMemoryEmulator;

/// <summary>
/// Detects potential version drift between the installed MongoDB.Driver version
/// and the version this emulator was tested against.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/drivers/csharp/current/
///   The MongoDB .NET driver evolves its interfaces and serialization behavior across versions.
///   This detector warns when the driver version falls outside the tested range.
/// </remarks>
public static class SdkVersionDriftDetector
{
    private static readonly Version MinTestedVersion = new(2, 28, 0, 0);
    private static readonly Version MaxTestedVersion = new(2, 30, 0, 0);

    /// <summary>
    /// Checks the current MongoDB.Driver version and returns a warning if it's outside the tested range.
    /// Returns null if the version is within the tested range.
    /// </summary>
    public static string? Check()
    {
        var driverAssembly = typeof(MongoDB.Driver.IMongoClient).Assembly;
        var version = driverAssembly.GetName().Version;

        if (version == null)
            return "Unable to determine MongoDB.Driver assembly version.";

        if (version < MinTestedVersion)
            return $"MongoDB.Driver {version} is older than the minimum tested version ({MinTestedVersion}). " +
                   "Some features may not work correctly.";

        if (version > MaxTestedVersion)
            return $"MongoDB.Driver {version} is newer than the maximum tested version ({MaxTestedVersion}). " +
                   "Some features may not work correctly. Consider updating MongoDB.InMemoryEmulator.";

        return null;
    }

    /// <summary>
    /// Checks the driver version and writes a warning to Console.Error if drift is detected.
    /// </summary>
    public static void WarnIfDrift()
    {
        var warning = Check();
        if (warning != null)
            Console.Error.WriteLine($"[MongoDB.InMemoryEmulator] WARNING: {warning}");
    }
}
