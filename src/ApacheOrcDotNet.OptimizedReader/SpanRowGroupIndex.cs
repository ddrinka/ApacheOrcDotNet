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
    public class StreamIndexDetail
    {
        public StreamDetail StreamDetail { get; set; }
        public List<RowGroupDetail> RowGroupDetails { get; } = new();
    }

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

    public static class SpanRowGroupIndex
    {
        public static IEnumerable<StreamIndexDetail> ReadRowGroupDetails(ReadOnlySequence<byte> inputSequence, List<StreamDetail> streamDetails, CompressionKind compressionKind)
        {
            bool compressionEnabled = compressionKind != CompressionKind.None;

            var rowIndex = Serializer.Deserialize<RowIndex>(inputSequence);
            var result = new List<StreamIndexDetail>();
            foreach(var stream in streamDetails)
            {
                result.Add(new StreamIndexDetail { StreamDetail = stream });
            }

            foreach(var entry in rowIndex.Entry)
            {
                var positions = entry.Positions.ToArray().AsSpan();
                for (int i = 0; i < streamDetails.Count; i++)
                {
                    var stream = streamDetails[i];
                    var streamPosition = stream.GetStreamPositionFromStreamType(compressionEnabled, positions);
                    var numConsumedPositions = stream.GetNumValuesInPositionListForStream(compressionEnabled);

                    result[i].RowGroupDetails.Add(new RowGroupDetail
                    {
                        Position = streamPosition,
                        Statistics = entry.Statistics
                    });
                    positions = positions[numConsumedPositions..];
                }
            }

            return result;
        }

        static int GetPositionsToSkip(int streamId, IEnumerable<StreamDetail> streamDetails, bool compressionEnabled)
        {
            int positionsToSkip = 0;
            foreach(var stream in streamDetails)
            {
                if (stream.StreamId >= streamId)
                    break;
                positionsToSkip += stream.GetNumValuesInPositionListForStream(compressionEnabled);
            }

            return positionsToSkip;
        }
    }
}
