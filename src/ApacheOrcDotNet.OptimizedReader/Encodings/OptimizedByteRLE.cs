using System;
using System.Buffers;

namespace ApacheOrcDotNet.OptimizedReader.Encodings
{
    public static class OptimizedByteRLE
    {
        public static int ReadValues(ref SequenceReader<byte> reader, Span<byte> outputValues)
        {
            var numReadValues = 0;

            if (!reader.TryRead(out var firstByte))
                return numReadValues;

            if (firstByte >= 0 && firstByte < 0x80) // A run
            {
                numReadValues = firstByte + 3;

                if (!reader.TryRead(out var repeatedByte))
                    throw new InvalidOperationException("Read past end of stream");

                for (int i = 0; i < numReadValues; i++)
                    outputValues[i] = repeatedByte;
            }
            else // Literals
            {
                numReadValues = 0x100 - firstByte;

                for (int i = 0; i < numReadValues; i++)
                {
                    if (!reader.TryRead(out var value))
                        throw new InvalidOperationException("Read past end of stream");

                    outputValues[i] = value;
                }
            }

            return numReadValues;
        }
    }
}
