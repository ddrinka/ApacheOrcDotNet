using ApacheOrcDotNet.Protocol;

namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public record StreamDetail(int StreamId, int ColumnId, long FileOffset, int Length, StreamKind StreamKind, ColumnEncodingKind EncodingKind, int DictionarySize)
    {
        public StreamPositions Positions { get; init; }
        public StreamRange Range { get; set; }
    }
}
