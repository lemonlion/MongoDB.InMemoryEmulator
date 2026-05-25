using System.Reflection;

namespace InMemoryEmulator.MongoDB;

/// <summary>
/// Detects potential version drift between the installed global::MongoDB.Driver version
/// and the version this emulator was tested against.
/// </summary>
/// <remarks>
/// Ref: https://www.mongodb.com/docs/drivers/csharp/current/
///   The MongoDB .NET driver evolves its interfaces and serialization behavior across versions.
///   This detector warns when the driver version falls outside the tested range.
/// </remarks>
public static class SdkVersionDriftDetector
{
    private static readonly Version MinTestedVersion = new(2, 0, 0, 0);
    private static readonly Version MaxTestedVersion = new(2, 30, 0, 0);

    /// <summary>
    /// Checks the current global::MongoDB.Driver version and returns a warning if it's outside the tested range.
    /// Returns null if the version is within the tested range.
    /// </summary>
    public static string? Check()
    {
        var driverAssembly = typeof(global::MongoDB.Driver.IMongoClient).Assembly;
        var version = driverAssembly.GetName().Version;

        if (version == null)
            return "Unable to determine global::MongoDB.Driver assembly version.";

        if (version < MinTestedVersion)
            return $"global::MongoDB.Driver {version} is older than the minimum tested version ({MinTestedVersion}). " +
                   "Some features may not work correctly.";

        if (version > MaxTestedVersion)
            return $"global::MongoDB.Driver {version} is newer than the maximum tested version ({MaxTestedVersion}). " +
                   "Some features may not work correctly. Consider updating InMemoryEmulator.MongoDB.";

        return null;
    }

    /// <summary>
    /// Checks the driver version and writes a warning to Console.Error if drift is detected.
    /// </summary>
    public static void WarnIfDrift()
    {
        var warning = Check();
        if (warning != null)
            Console.Error.WriteLine($"[InMemoryEmulator.MongoDB] WARNING: {warning}");
    }
}
