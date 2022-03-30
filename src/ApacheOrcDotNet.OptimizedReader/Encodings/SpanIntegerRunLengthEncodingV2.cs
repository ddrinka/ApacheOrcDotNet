using System;
using System.Buffers;

namespace ApacheOrcDotNet.OptimizedReader.Encodings
{
    public static class SpanIntegerRunLengthEncodingV2
    {
        enum EncodingType { ShortRepeat, Direct, PatchedBase, Delta }

        public static int ReadValues(ReadOnlySequence<byte> input, Position position, bool isSigned, Span<long> values)
        {
            throw new NotImplementedException();
        }
    }
}
