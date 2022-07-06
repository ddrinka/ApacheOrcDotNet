using ApacheOrcDotNet.Protocol;
using System;

namespace ApacheOrcDotNet.OptimizedReader
{
    public class OrcFileProperties
    {
        public OrcFileProperties(CompressionKind compressionKind, int compressionBlockSize, int maxValuesToRead, int maxCompressedBufferLength = 0, int maxDecompressedBufferLength = 0)
        {
            CompressionKind = compressionKind;
            CompressionBlockSize = compressionBlockSize;
            MaxValuesToRead = maxValuesToRead;

            // When requesting ranges to be decompressed,
            // for some edge cases, we may need to load up to two chunks of data
            // (to account for blocks that had data saved into the next row group)
            if (maxCompressedBufferLength <= 0)
                maxCompressedBufferLength = compressionBlockSize * 2;

            // When decompressing data, we only know the total size
            // after the data is decopressed. To guarantee we will not
            // run out of space during this process, we allocate a 25 Mb buffer for that.
            // This is an arbirary number based on the maximum size decompressed using production data (~10Mb).
            if (maxDecompressedBufferLength <= 0)
                maxDecompressedBufferLength = 25 * 1024 * 1024;

            MaxCompressedBufferLength = maxCompressedBufferLength;
            MaxDecompressedBufferLength = maxDecompressedBufferLength;
        }

        public static DateTime OrcEpoch = new DateTime(2015, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        public static DateTime UnixEpochUtc = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        public static DateTime UnixEpochUnspecified = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);

        public CompressionKind CompressionKind { get; }
        public int CompressionBlockSize { get; }
        public int MaxValuesToRead { get; }
        public int MaxCompressedBufferLength { get; }
        public int MaxDecompressedBufferLength { get; }
    }
}
