using ApacheOrcDotNet.Protocol;

namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public record StreamDetails(int StreamId, int ColumnId, long FileOffset, int Length, StreamKind StreamKind, ColumnEncodingKind EncodingKind, int DictionarySize);

    public record BufferStream
    {
        public StreamDetails Details { get; init; }
        public StreamPositions Positions { get; init; }
        public byte[] CompressedBuffer { get; init; }
        public byte[] DecompressedBuffer { get; init; }
        public int DecompressedBufferLength { get; set; }
    }
}
