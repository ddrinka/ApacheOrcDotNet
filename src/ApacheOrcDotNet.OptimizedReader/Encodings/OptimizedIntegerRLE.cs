using ApacheOrcDotNet.Encodings;
using System;
using System.Buffers;
using System.IO;

namespace ApacheOrcDotNet.OptimizedReader.Encodings
{
    public static class OptimizedIntegerRLE
    {
        enum EncodingType
        {
            ShortRepeat = 0,
            Direct = 1,
            PatchedBase = 2,
            Delta = 3
        }

        public static int ReadValues(ref BufferReader reader, bool isSigned, Span<long> outputValues)
        {
            var numReadValues = 0;

            if (!reader.TryRead(out var firstByte))
                return numReadValues;

            var encodingType = (EncodingType)((firstByte >> 6) & 0x3);

            switch (encodingType)
            {
                case EncodingType.ShortRepeat:
                    ReadShortRepeatValues(firstByte, isSigned, ref reader, outputValues, ref numReadValues);
                    break;
                case EncodingType.Direct:
                    ReadDirectValues(firstByte, isSigned, ref reader, outputValues, ref numReadValues);
                    break;
                case EncodingType.PatchedBase:
                    ReadPatchedBaseValues(firstByte, isSigned: false, ref reader, outputValues, ref numReadValues);
                    break;
                case EncodingType.Delta:
                    ReadDeltaValues(firstByte, isSigned, ref reader, outputValues, ref numReadValues);
                    break;
                default:
                    throw new InvalidOperationException($"Invalid encoding type: {encodingType}");
            };

            return numReadValues;
        }

        private static int ReadShortRepeatValues(int firstByte, bool isSigned, ref BufferReader reader, Span<long> outputValues, ref int numReadValues)
        {
            var width = ((firstByte >> 3) & 0x7) + 1;
            numReadValues = (firstByte & 0x7) + 3;

            var repeatingValue = ReadLongBE(ref reader, width, isSigned);

            for (int i = 0; i < numReadValues; i++)
                outputValues[i] = repeatingValue;

            return width;
        }

        private static void ReadDirectValues(int firstByte, bool isSigned, ref BufferReader reader, Span<long> outputValues, ref int numReadValues)
        {
            var encodedWidth = (firstByte >> 1) & 0x1f;
            var width = encodedWidth.DecodeDirectWidth();
            numReadValues = (firstByte & 0x1) << 8;

            if (!reader.TryRead(out var nextByte))
                throw new InvalidOperationException("Read past end of stream");

            numReadValues |= nextByte;
            numReadValues += 1;

            ReadBitpackedIntegers(ref reader, isSigned, width, numReadValues, outputValues);
        }

        private static void ReadPatchedBaseValues(int firstByte, bool isSigned, ref BufferReader reader, Span<long> outputValues, ref int numReadValues)
        {
            var encodedWidth = (firstByte >> 1) & 0x1f;
            var directBitWidth = encodedWidth.DecodeDirectWidth();
            numReadValues = (firstByte & 0x1) << 8;

            if (!reader.TryRead(out var nextByte))
                throw new InvalidOperationException("Read past end of stream");
            numReadValues |= nextByte;
            numReadValues += 1;

            if (!reader.TryRead(out nextByte))
                throw new InvalidOperationException("Read past end of stream");
            var baseValueWidth = ((nextByte >> 5) & 0x7) + 1;
            var encodedPatchWidth = nextByte & 0x1f;
            var patchWidth = encodedPatchWidth.DecodeDirectWidth();

            if (!reader.TryRead(out nextByte))
                throw new InvalidOperationException("Read past end of stream");
            var patchGapWidth = ((nextByte >> 5) & 0x7) + 1;
            var patchListLength = nextByte & 0x1f;

            long baseValue = ReadLongBE(ref reader, baseValueWidth);
            long msbMask = (1L << ((baseValueWidth * 8) - 1));
            if ((baseValue & msbMask) != 0)
            {
                baseValue = baseValue & ~msbMask;
                baseValue = -baseValue;
            }

            //Buffer all the values so we can patch thems
            ReadBitpackedIntegers(ref reader, isSigned, directBitWidth, numReadValues, outputValues);

            if (patchGapWidth + patchWidth > 64)
                throw new InvalidDataException($"{nameof(patchGapWidth)} ({patchGapWidth}) + {nameof(patchWidth)} ({patchWidth}) > 64");

            var patchListValues = ArrayPool<long>.Shared.Rent(patchListLength);
            var patchListValuesSpan = patchListValues.AsSpan().Slice(0, patchListLength);

            try
            {
                var gap = 0L;
                var patch = 0L;
                var patchIndex = 0;
                var patchListWidth = BitManipulation.FindNearestDirectWidth(patchWidth + patchGapWidth);
                ReadBitpackedIntegers(ref reader, isSigned, patchListWidth, patchListLength, patchListValuesSpan);
                GetNextPatch(patchListValuesSpan, ref patchIndex, ref gap, out patch, patchWidth, (1L << patchWidth) - 1);

                for (int i = 0; i < numReadValues; i++)
                {
                    if (i == gap)
                    {
                        var patchedValue = outputValues[i] | (patch << directBitWidth);
                        outputValues[i] = baseValue + patchedValue;

                        if (patchIndex < patchListLength)
                            GetNextPatch(patchListValuesSpan, ref patchIndex, ref gap, out patch, patchWidth, (1L << patchWidth) - 1);
                    }
                    else
                        outputValues[i] = baseValue + outputValues[i];
                }
            }
            finally
            {
                ArrayPool<long>.Shared.Return(patchListValues, clearArray: false);
            }
        }

        private static void ReadDeltaValues(int firstByte, bool isSigned, ref BufferReader reader, Span<long> outputValues, ref int numReadValues)
        {
            int index = 0;
            int bitWidth = 0;
            int encodedWidth = (firstByte >> 1) & 0x1f;
            if (encodedWidth != 0) // EncodedWidth 0 means Width 0 for Delta
                bitWidth = encodedWidth.DecodeDirectWidth();
            numReadValues = (firstByte & 0x1) << 8;

            if (!reader.TryRead(out var nextByte))
                throw new InvalidOperationException("Read past end of stream");
            numReadValues |= nextByte;

            if (isSigned)
                outputValues[index++] = ReadVarIntSigned(ref reader);
            else
                outputValues[index++] = ReadVarIntUnsigned(ref reader);

            var deltaBase = ReadVarIntSigned(ref reader);
            if (bitWidth == 0)
            {
                // Uses a fixed delta base for every value
                for (int i = 0; i <= numReadValues; i++)
                {
                    outputValues[index++] = outputValues[index - 2] + deltaBase;
                }
            }
            else
            {
                outputValues[index++] = outputValues[index - 2] + deltaBase;

                var deltaValues = ArrayPool<long>.Shared.Rent(numReadValues - 1);
                var deltaValuesSpan = deltaValues.AsSpan().Slice(0, numReadValues - 1);

                try
                {
                    ReadBitpackedIntegers(ref reader, isSigned: false, bitWidth, numReadValues - 1, deltaValuesSpan);

                    for (int i = 0; i < deltaValuesSpan.Length; i++)
                    {
                        outputValues[index++] = deltaBase > 0
                            ? outputValues[index - 2] + deltaValuesSpan[i]
                            : outputValues[index - 2] - deltaValuesSpan[i];
                    }
                }
                finally
                {
                    ArrayPool<long>.Shared.Return(deltaValues, clearArray: false);
                }
            }

            numReadValues++; // Delta lengths start at 0
        }

        private static void GetNextPatch(Span<long> patchListValues, ref int patchIndex, ref long gap, out long patch, int patchWidth, long patchMask)
        {
            while (true)
            {
                var raw = patchListValues[patchIndex++];
                var curGap = (long)((ulong)raw >> patchWidth);
                patch = raw & patchMask;

                if (curGap != 255 || patch != 0)
                {
                    gap += curGap;
                    break;
                }

                gap += 255;
            }
        }

        private static void ReadBitpackedIntegers(ref BufferReader reader, bool isSigned, int bitWidth, int count, Span<long> outputValues)
        {
            byte currentByte = 0;
            int bitsAvailable = 0;
            for (int i = 0; i < count; i++)
            {
                ulong result = 0;
                int neededBits = bitWidth;
                while (neededBits > bitsAvailable)
                {
                    result <<= bitsAvailable; // Make space for incoming bits
                    result |= currentByte & ((1u << bitsAvailable) - 1); // OR in the bits
                    neededBits -= bitsAvailable;

                    if (!reader.TryRead(out var nextByte))
                        throw new InvalidOperationException("Read past end of stream");
                    currentByte = nextByte;

                    bitsAvailable = 8;
                }

                if (neededBits > 0) // Left over bits
                {
                    result <<= neededBits;
                    bitsAvailable -= neededBits;
                    result |= ((ulong)currentByte >> bitsAvailable) & ((1ul << neededBits) - 1);
                }

                outputValues[i] = isSigned
                    ? ((long)result).ZigzagDecode()
                    : (long)result;
            }
        }

        private static long ReadLongBE(ref BufferReader reader, int numBytes, bool isSigned = false)
        {
            long result = 0;

            for (int i = numBytes - 1; i >= 0; i--)
            {
                if (!reader.TryRead(out var nextByte))
                    throw new InvalidOperationException("Read past end of stream");

                result |= (long)nextByte << (i * 8);
            }

            return isSigned
                ? result.ZigzagDecode()
                : result;
        }

        private static long ReadVarIntSigned(ref BufferReader reader)
        {
            var unsigned = ReadVarIntUnsigned(ref reader);
            return unsigned.ZigzagDecode();
        }

        private static long ReadVarIntUnsigned(ref BufferReader reader)
        {
            long result = 0;
            int bitCount = 0;

            while (true)
            {
                if (!reader.TryRead(out var currentByte))
                    throw new InvalidOperationException("Read past end of stream");

                result |= ((long)currentByte & 0x7f) << bitCount;
                bitCount += 7;

                // Done when the high bit is set
                if (currentByte < 0x80)
                    break;
            }

            return result;
        }
    }
}
