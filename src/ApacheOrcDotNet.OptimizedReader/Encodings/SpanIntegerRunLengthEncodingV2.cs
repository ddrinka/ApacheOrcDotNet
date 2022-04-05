using ApacheOrcDotNet.Encodings;
using System;
using System.IO;

namespace ApacheOrcDotNet.OptimizedReader.Encodings
{
    public static class SpanIntegerRunLengthEncodingV2
    {
        enum EncodingType { ShortRepeat, Direct, PatchedBase, Delta }

        public static int ReadValues(ReadOnlySpan<byte> input, Position position, bool isSigned, Span<long> values)
        {
            var firstByte = input[0];
            var encodingType = (EncodingType)((firstByte >> 6) & 0x3);

            return encodingType switch
            {
                EncodingType.ShortRepeat => ReadShortRepeatValues(firstByte, isSigned, input.Slice(1), values),
                EncodingType.Direct => ReadDirectValues(firstByte, isSigned, input.Slice(1), values),
                EncodingType.PatchedBase => ReadPatchedBaseValues(firstByte, isSigned, input.Slice(1), values),
                EncodingType.Delta => ReadDeltaValues(firstByte, isSigned, input.Slice(1), values),
                _ => throw new InvalidOperationException($"Invalid encoding type: {encodingType}")
            };
        }

        private static int ReadShortRepeatValues(int firstByte, bool isSigned, ReadOnlySpan<byte> input, Span<long> values)
        {
            var width = ((firstByte >> 3) & 0x7) + 1;
            var repeatCount = (firstByte & 0x7) + 3;
            var value = input.ReadLongBE(width);

            if (isSigned)
                value = value.ZigzagDecode();

            for (int i = 0; i < repeatCount; i++)
                values[i] = value;

            return width;
        }

        private static int ReadDirectValues(int firstByte, bool isSigned, ReadOnlySpan<byte> input, Span<long> values)
        {
            var offset = 0;
            var encodedWidth = (firstByte >> 1) & 0x1f;
            var width = encodedWidth.DecodeDirectWidth();
            int length = (firstByte & 0x1) << 8;

            length |= input[offset++];
            length += 1;

            offset += input.Slice(offset).ReadBitpackedIntegers(isSigned, width, length, values);

            return offset;
        }

        private static int ReadPatchedBaseValues(int firstByte, bool isSigned, ReadOnlySpan<byte> input, Span<long> values)
        {
            var offset = 0;
            var encodedWidth = (firstByte >> 1) & 0x1f;
            var width = encodedWidth.DecodeDirectWidth();
            int length = (firstByte & 0x1) << 8;
            length |= input[offset++];
            length += 1;

            var thirdByte = input[offset++];
            var baseValueWidth = ((thirdByte >> 5) & 0x7) + 1;
            var encodedPatchWidth = thirdByte & 0x1f;
            var patchWidth = encodedPatchWidth.DecodeDirectWidth();

            var fourthByte = input[offset++];
            var patchGapWidth = ((fourthByte >> 5) & 0x7) + 1;
            var patchListLength = fourthByte & 0x1f;

            long baseValue = input.Slice(offset).ReadLongBE(baseValueWidth);
            long msbMask = (1L << ((baseValueWidth * 8) - 1));
            if ((baseValue & msbMask) != 0)
            {
                baseValue = baseValue & ~msbMask;
                baseValue = -baseValue;
            }
            offset += baseValueWidth;

            //Buffer all the values so we can patch them
            offset += input.Slice(offset).ReadBitpackedIntegers(isSigned, width, length, values);

            if (patchGapWidth + patchWidth > 64)
                throw new InvalidDataException($"{nameof(patchGapWidth)} ({patchGapWidth}) + {nameof(patchWidth)} ({patchWidth}) > 64");

            Span<long> patchListValues = stackalloc long[patchListLength];
            var patchListWidth = BitManipulation.FindNearestDirectWidth(patchWidth + patchGapWidth);
            offset += input.Slice(offset).ReadBitpackedIntegers(isSigned, patchListWidth, patchListLength, patchListValues);

            int patchIndex = 0;
            long gap = 0;
            long patch;
            GetNextPatch(patchListValues, ref patchIndex, ref gap, out patch, patchWidth, (1L << patchWidth) - 1);

            for (int i = 0; i < length; i++)
            {
                if (i == gap)
                {
                    var patchedValue = values[i] | (patch << width);
                    values[i] = baseValue + patchedValue;

                    if (patchIndex < patchListLength)
                        GetNextPatch(patchListValues, ref patchIndex, ref gap, out patch, patchWidth, (1L << patchWidth) - 1);
                }
                else
                    values[i] = baseValue + values[i];
            }

            return offset;
        }

        private static int ReadDeltaValues(int firstByte, bool isSigned, ReadOnlySpan<byte> input, Span<long> values)
        {
            var offset = 0;
            var valueIndex = 0;
            var encodedWidth = (firstByte >> 1) & 0x1f;
            int width = 0;
            if (encodedWidth != 0) //EncodedWidth 0 means Width 0 for Delta
                width = encodedWidth.DecodeDirectWidth();
            int length = (firstByte & 0x1) << 8;
            length |= input[offset++];
            //Delta lengths start at 0

            if (isSigned)
                values[valueIndex++] = input.ReadVarIntSigned(ref offset);
            else
                values[valueIndex++] = input.ReadVarIntUnsigned(ref offset);

            var deltaBase = input.ReadVarIntSigned(ref offset);
            if (width == 0)
            {
                //Uses a fixed delta base for every value
                for (int i = 0; i < length; i++)
                {
                    values[valueIndex++] = values[valueIndex - 2] + deltaBase;
                }
            }
            else
            {
                values[valueIndex++] = values[valueIndex - 2] + deltaBase;

                Span<long> deltaValues = stackalloc long[length - 1];
                offset += input.Slice(offset).ReadBitpackedIntegers(isSigned, width, length - 1, deltaValues);

                for (int index = 0; index < deltaValues.Length; index++)
                {
                    if (deltaBase > 0)
                        values[valueIndex++] = values[valueIndex - 2] + deltaValues[index];
                    else
                        values[valueIndex++] = values[valueIndex - 2] - deltaValues[index];
                }
            }

            return offset;
        }

        private static void GetNextPatch(Span<long> patchListValues, ref int patchIndex, ref long gap, out long patch, int patchWidth, long patchMask)
        {
            var raw = patchListValues[patchIndex];
            patchIndex++;
            long curGap = (long)((ulong)raw >> patchWidth);
            patch = raw & patchMask;
            while (curGap == 255 && patch == 0)
            {
                gap += 255;
                raw = patchListValues[patchIndex];
                patchIndex++;
                curGap = (long)((ulong)raw >> patchWidth);
                patch = raw & patchMask;
            }
            gap += curGap;
        }
    }
}
