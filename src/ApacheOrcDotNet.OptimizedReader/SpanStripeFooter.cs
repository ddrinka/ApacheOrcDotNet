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
    public class StreamDetail
    {
        public int StreamId { get; set; }
        public int ColumnId { get; set; }
        public long FileOffset { get; set; }
        public int Length { get; set; }
        public ColumnTypeKind ColumnType { get; set; }
        public StreamKind StreamKind { get; set; }
        public ColumnEncodingKind EncodingKind { get; set; }
    }

    public static class SpanStripeFooter
    {
        public static IEnumerable<StreamDetail> ReadStreamDetails(ReadOnlySequence<byte> inputSequence, IEnumerable<ColumnDetail> columnDetails, long stripeOffset)
        {
            var stripeFooter = Serializer.Deserialize<StripeFooter>(inputSequence);

            return stripeFooter.Streams.Select((stream, i) =>
            {
                var result = new StreamDetail
                {
                    StreamId = i,
                    ColumnId = (int)stream.Column,
                    FileOffset = stripeOffset,
                    Length = (int)stream.Length,
                    ColumnType = columnDetails.Single(c => c.ColumnId == stream.Column).ColumnType,
                    StreamKind = stream.Kind,
                    EncodingKind = stripeFooter.Columns[(int)stream.Column].Kind
                };

                stripeOffset += (long)stream.Length;

                return result;
            }).ToList();
        }
    }
}
