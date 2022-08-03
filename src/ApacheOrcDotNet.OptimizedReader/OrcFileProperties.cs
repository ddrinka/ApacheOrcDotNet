using ApacheOrcDotNet.Protocol;

namespace ApacheOrcDotNet.OptimizedReader
{
    public class OrcFileProperties
    {
        public OrcFileProperties(CompressionKind compressionKind, int decompressedChunkMaxLength, int maxValuesToRead, int numPreAllocatedDecompressionChunks)
        {
            CompressionKind = compressionKind;
            DecompressedChunkMaxLength = decompressedChunkMaxLength;
            MaxValuesToRead = maxValuesToRead;
            ReusableBufferLength = decompressedChunkMaxLength * numPreAllocatedDecompressionChunks;
        }

        public CompressionKind CompressionKind { get; }
        public int DecompressedChunkMaxLength { get; }
        public int MaxValuesToRead { get; }
        public int ReusableBufferLength { get; }
    }
}
