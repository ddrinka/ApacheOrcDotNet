using ApacheOrcDotNet.Protocol;

namespace ApacheOrcDotNet.OptimizedReader
{
    public class OrcFileProperties
    {
        public OrcFileProperties(CompressionKind compressionKind, int compressionBlockSize, int maxValuesToRead, int maxCompressedBufferLength = 0, int maxDecompressedBufferLength = 0)
        {
            CompressionKind = compressionKind;
            CompressionBlockSize = compressionBlockSize;
            MaxValuesToRead = maxValuesToRead;

            if (maxCompressedBufferLength <= 0)
                maxCompressedBufferLength = 25 * 1024 * 1024;

            if (maxDecompressedBufferLength <= 0)
                maxDecompressedBufferLength = 25 * 1024 * 1024;

            MaxCompressedBufferLength = maxCompressedBufferLength;
            MaxDecompressedBufferLength = maxDecompressedBufferLength;
        }

        public CompressionKind CompressionKind { get; }
        public int CompressionBlockSize { get; }
        public int MaxValuesToRead { get; }
        public int MaxCompressedBufferLength { get; }
        public int MaxDecompressedBufferLength { get; }
    }
}
