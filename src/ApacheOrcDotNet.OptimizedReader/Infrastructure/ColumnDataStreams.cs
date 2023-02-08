using ApacheOrcDotNet.Protocol;

namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public class ColumnDataStreams
    {
        public ColumnEncodingKind EncodingKind { get; set; }

        public StreamDetail Data { get; set; }
        public StreamDetail DictionaryData { get; set; }
        public StreamDetail Length { get; set; }
        public StreamDetail Present { get; set; }
        public StreamDetail Secondary { get; set; }
    }
}
