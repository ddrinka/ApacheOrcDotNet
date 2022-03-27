using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using ApacheOrcDotNet.Statistics;
using ProtoBuf;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader
{
    public class RowGroupDetail
    {
        public ColumnStatistics Statistics { get; set; }
        public StreamPosition Position { get; set; }
    }

    public class StreamPosition
    {
        public long ChunkFileOffset { get; set; }
        public int? DecompressedOffset { get; set; }
        public int ValueOffset { get; set; }
        public int? ValueOffset2 { get; set; }  //Used for bitstreams
    }

    public sealed class SpanRowGroupIndex
    {
        readonly RowIndex _rowIndex;

        public SpanRowGroupIndex(ReadOnlySequence<byte> inputSequence)
        {
            _rowIndex = Serializer.Deserialize<RowIndex>(inputSequence);
        }

        public int Count => _rowIndex.Entry.Count;

        public ColumnStatistics GetColumnStatistics(int rowGroupId) => _rowIndex.Entry[rowGroupId].Statistics;
    }
}
