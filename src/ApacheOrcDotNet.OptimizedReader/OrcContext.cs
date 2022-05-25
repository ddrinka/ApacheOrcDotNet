using ApacheOrcDotNet.Protocol;

namespace ApacheOrcDotNet.OptimizedReader
{
    public class OrcContextNew
    {
        public OrcContextNew(CompressionKind compressionKind, int compressionBlockSize, int maxValuesToRead)
        {
            CompressionKind = compressionKind;
            CompressionBlockSize = compressionBlockSize;
            MaxValuesToRead = maxValuesToRead;
        }

        public CompressionKind CompressionKind { get; }
        public int CompressionBlockSize { get; }
        public int MaxValuesToRead { get; }
    }
}
