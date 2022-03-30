using ApacheOrcDotNet.Protocol;
using ApacheOrcDotNet.Statistics;
using ProtoBuf;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;

namespace ApacheOrcDotNet.OptimizedReader
{
    public record RowGroupDetail(ColumnStatistics Statistics, List<StreamPosition> StreamPositions);
    public record StreamPosition(StreamDetail Stream, Position Position);
    public record Position(long ChunkFileOffset, int? DecompressedOffset, int ValueOffset, int? ValueOffset2);

    public static class SpanRowGroupIndex
    {
        public static IEnumerable<RowGroupDetail> ReadRowGroupDetails(ReadOnlySequence<byte> inputSequence, List<StreamDetail> streamDetails, CompressionKind compressionKind)
        {
            bool compressionEnabled = compressionKind != CompressionKind.None;

            var rowIndex = Serializer.Deserialize<RowIndex>(inputSequence);

            foreach(var entry in rowIndex.Entry)
            {
                var streamPositions = new List<StreamPosition>();
                var positions = entry.Positions.ToArray().AsSpan();
                foreach(var stream in streamDetails)
                {
                    var numConsumedPositions = stream.GetNumValuesInPositionListForStream(compressionEnabled);
                    if (numConsumedPositions == 0)
                        continue;
                    var streamPosition = stream.GetStreamPositionFromStreamType(compressionEnabled, positions);

                    streamPositions.Add(new StreamPosition(Stream: stream, Position: streamPosition));
                    positions = positions[numConsumedPositions..];
                }
                if (positions.Length != 0)
                    throw new InvalidDataException($"Some position records were not consumed. ColumnType={streamDetails[0].ColumnType} StreamId={streamDetails[0].StreamId}");

                yield return new RowGroupDetail(Statistics: entry.Statistics, StreamPositions: streamPositions);
            }
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
