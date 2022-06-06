﻿using ApacheOrcDotNet.Protocol;
using ProtoBuf;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;

namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public static class SpanStripeFooter
    {
        public static IEnumerable<StreamDetails> ReadStreamDetails(ReadOnlySequence<byte> inputSequence, long stripeOffset)
        {
            var stripeFooter = Serializer.Deserialize<StripeFooter>(inputSequence);

            return stripeFooter.Streams.Select((stream, i) =>
            {
                var result = new StreamDetails
                (
                    StreamId: i,
                    ColumnId: (int)stream.Column,
                    FileOffset: stripeOffset,
                    Length: (int)stream.Length,
                    StreamKind: stream.Kind,
                    EncodingKind: stripeFooter.Columns[(int)stream.Column].Kind,
                    DictionarySize: (int)stripeFooter.Columns[(int)stream.Column].DictionarySize
                );

                stripeOffset += (long)stream.Length;

                return result;
            }).ToList();
        }
    }
}
