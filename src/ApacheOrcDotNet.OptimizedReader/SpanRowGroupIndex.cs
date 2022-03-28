using ApacheOrcDotNet.Protocol;
using ApacheOrcDotNet.Statistics;
using ProtoBuf;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;

namespace ApacheOrcDotNet.OptimizedReader
{
    public record StreamIndexDetail(StreamDetail StreamDetail, List<RowGroupDetail> RowGroupDetails);
    public record RowGroupDetail(ColumnStatistics Statistics, StreamPosition Position);
    public record StreamPosition(long ChunkFileOffset, int? DecompressedOffset, int ValueOffset, int? ValueOffset2);

    public static class SpanRowGroupIndex
    {
        public static IEnumerable<StreamIndexDetail> ReadRowGroupDetails(ReadOnlySequence<byte> inputSequence, List<StreamDetail> streamDetails, CompressionKind compressionKind)
        {
            bool compressionEnabled = compressionKind != CompressionKind.None;

            var rowIndex = Serializer.Deserialize<RowIndex>(inputSequence);
            var result = new List<StreamIndexDetail>();
            foreach(var stream in streamDetails)
            {
                result.Add(new StreamIndexDetail(StreamDetail: stream, RowGroupDetails: new()));
            }

            foreach(var entry in rowIndex.Entry)
            {
                var positions = entry.Positions.ToArray().AsSpan();
                for (int i = 0; i < streamDetails.Count; i++)
                {
                    var stream = streamDetails[i];
                    var numConsumedPositions = stream.GetNumValuesInPositionListForStream(compressionEnabled);
                    if (numConsumedPositions == 0)
                        continue;
                    var streamPosition = stream.GetStreamPositionFromStreamType(compressionEnabled, positions);

                    result[i].RowGroupDetails.Add(new RowGroupDetail
                    (
                        Position: streamPosition,
                        Statistics: entry.Statistics
                    ));
                    positions = positions[numConsumedPositions..];
                }
                if (positions.Length != 0)
                    throw new InvalidDataException($"Some position records were not consumed. ColumnType={streamDetails[0].ColumnType} StreamId={streamDetails[0].StreamId}");
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
