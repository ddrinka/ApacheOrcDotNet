using System;

namespace ApacheOrcDotNet.OptimizedReader
{
    public static class Constants
    {
        public static DateTime OrcEpoch = new(2015, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        public static DateTime UnixEpochUtc = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        public static DateTime UnixEpochUnspecified = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);
    }
}
