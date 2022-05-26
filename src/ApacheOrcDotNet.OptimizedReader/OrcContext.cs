using ApacheOrcDotNet.Protocol;

namespace ApacheOrcDotNet.OptimizedReader
{
    public class OrcContext
    {
        public OrcContext(CompressionKind compressionKind, int compressionBlockSize, int maxValuesToRead, int maxCompressedBufferLength = 0, int maxDecompresseBufferLength = 0)
        {
            CompressionKind = compressionKind;
            CompressionBlockSize = compressionBlockSize;
            MaxValuesToRead = maxValuesToRead;

            if (maxCompressedBufferLength <= 0)
                maxCompressedBufferLength = 25 * 1024 * 1024;

            if (maxDecompresseBufferLength <= 0)
                maxDecompresseBufferLength = 25 * 1024 * 1024;

            MaxCompressedBufferLength = maxCompressedBufferLength;
            MaxDecompresseBufferLength = maxDecompresseBufferLength;
        }

        public CompressionKind CompressionKind { get; }
        public int CompressionBlockSize { get; }
        public int MaxValuesToRead { get; }
        public int MaxCompressedBufferLength { get; }
        public int MaxDecompresseBufferLength { get; }
    }
}
