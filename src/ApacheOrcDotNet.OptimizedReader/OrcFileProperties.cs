using ApacheOrcDotNet.Protocol;
using System;

namespace ApacheOrcDotNet.OptimizedReader
{
    public class OrcFileProperties
    {
        public OrcFileProperties(CompressionKind compressionKind, int compressionBlockSize, int maxValuesToRead, int reusableBufferLength)
        {
            CompressionKind = compressionKind;
            CompressionBlockSize = compressionBlockSize;
            MaxValuesToRead = maxValuesToRead;
            ReusableBufferLength = reusableBufferLength;
        }

        public static DateTime OrcEpoch = new DateTime(2015, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        public static DateTime UnixEpochUtc = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        public static DateTime UnixEpochUnspecified = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);

        public CompressionKind CompressionKind { get; }
        public int CompressionBlockSize { get; }
        public int MaxValuesToRead { get; }
        public int ReusableBufferLength { get; }
    }
}
