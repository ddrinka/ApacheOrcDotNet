﻿using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.OptimizedReader.Encodings;
using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using System;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    public abstract class BaseColumnBuffer<TOutput>
    {
        private protected readonly IByteRangeProvider _byteRangeProvider;
        private protected readonly OrcFileProperties _orcFileProperties;
        private protected readonly OrcColumn _column;
        private protected readonly TOutput[] _values;

        private protected int _numValuesRead;

        private StreamRange _lastRange;

        public BaseColumnBuffer(IByteRangeProvider byteRangeProvider, OrcFileProperties orcFileProperties, OrcColumn column)
        {
            _byteRangeProvider = byteRangeProvider;
            _orcFileProperties = orcFileProperties;
            _column = column;
            _values = new TOutput[_orcFileProperties.MaxValuesToRead];
        }

        public OrcColumn Column => _column;
        public ReadOnlySpan<TOutput> Values => _values.AsSpan()[.._numValuesRead];

        public abstract Task LoadDataAsync(int stripeId, ColumnDataStreams streams);

        public void Reset() => _numValuesRead = 0;

        private protected void ReadByteStream(StreamDetail stream, ReadOnlySpan<byte> decompressedBuffer, Span<byte> rleBuffer, Span<byte> outputValues, out int numValuesRead)
        {
            numValuesRead = 0;

            if (stream == null)
                return;

            var numSkipped = 0;
            var bufferReader = new BufferReader(GetDataStream(stream, decompressedBuffer));

            while (!bufferReader.Complete)
            {
                var numByteValuesRead = OptimizedByteRLE.ReadValues(ref bufferReader, rleBuffer);

                for (int idx = 0; idx < numByteValuesRead; idx++)
                {
                    if (numSkipped++ < stream.Positions.ValuesToSkip)
                        continue;

                    outputValues[numValuesRead++] = rleBuffer[idx];

                    if (numValuesRead >= outputValues.Length)
                        return;
                }
            }
        }

        private protected void ReadBooleanStream(StreamDetail stream, ReadOnlySpan<byte> decompressedBuffer, Span<byte> rleBuffer, Span<bool> outputValues, out int numValuesRead)
        {
            numValuesRead = 0;

            if (stream == null)
                return;

            var isFirstByte = true;
            var numSkippedBytes = 0;
            var numOfBytesToSkip = stream.Positions.ValuesToSkip;
            var bufferReader = new BufferReader(GetDataStream(stream, decompressedBuffer));

            if (outputValues.Length < 8)
                throw new ArgumentException($"Boolean buffers length must be greater or equal to 8.");

            Span<bool> rleValues = stackalloc bool[8];

            while (!bufferReader.Complete)
            {
                var numByteValuesRead = OptimizedByteRLE.ReadValues(ref bufferReader, rleBuffer);

                for (int idx = 0; idx < numByteValuesRead; idx++)
                {
                    if (numSkippedBytes++ < numOfBytesToSkip)
                        continue;

                    var decodedByte = rleBuffer[idx];

                    rleValues[0] = (decodedByte & 128) != 0;
                    rleValues[1] = (decodedByte & 64) != 0;
                    rleValues[2] = (decodedByte & 32) != 0;
                    rleValues[3] = (decodedByte & 16) != 0;
                    rleValues[4] = (decodedByte & 8) != 0;
                    rleValues[5] = (decodedByte & 4) != 0;
                    rleValues[6] = (decodedByte & 2) != 0;
                    rleValues[7] = (decodedByte & 1) != 0;

                    if (isFirstByte)
                    {
                        isFirstByte = false;

                        var firstValues = rleValues[stream.Positions.RemainingBits..];

                        numValuesRead += firstValues.Length;

                        firstValues.CopyTo(outputValues);
                    }
                    else
                    {
                        var targetBuffer = outputValues[numValuesRead..];

                        if (numValuesRead + rleValues.Length >= outputValues.Length)
                        {
                            var source = rleValues[..(outputValues.Length - numValuesRead)];
                            numValuesRead += source.Length;
                            source.CopyTo(targetBuffer);
                            return;
                        }

                        numValuesRead += rleValues.Length;

                        rleValues.CopyTo(targetBuffer);
                    }
                }
            }
        }

        private protected void ReadNumericStream(StreamDetail stream, ReadOnlySpan<byte> decompressedBuffer, Span<long> rleBuffer, bool isSigned, Span<long> outputValues, out int numValuesRead)
        {
            numValuesRead = 0;

            if (stream == null)
                return;

            var numSkipped = 0;
            var bufferReader = new BufferReader(GetDataStream(stream, decompressedBuffer));

            while (!bufferReader.Complete)
            {
                var numNewValuesRead = OptimizedIntegerRLE.ReadValues(ref bufferReader, isSigned, rleBuffer);

                for (int idx = 0; idx < numNewValuesRead; idx++)
                {
                    if (numSkipped++ < stream.Positions.ValuesToSkip)
                        continue;

                    outputValues[numValuesRead++] = rleBuffer[idx];

                    if (numValuesRead >= outputValues.Length)
                        return;
                }
            }
        }

        private protected static void ReadVarIntStream(StreamDetail stream, ReadOnlySpan<byte> decompressedBuffer, Span<long> outputValues, out int numValuesRead)
        {
            numValuesRead = 0;

            if (stream == null)
                return;

            int numSkipped = 0;
            var bufferReader = new BufferReader(GetDataStream(stream, decompressedBuffer));

            while (!bufferReader.Complete)
            {
                var varInt = ReadVarInt(ref bufferReader);

                if (!varInt.HasValue)
                    return;

                if (numSkipped++ < stream.Positions.ValuesToSkip)
                    continue;

                outputValues[numValuesRead++] = varInt.Value;

                if (numValuesRead >= outputValues.Length)
                    return;
            }
        }

        private protected static void CheckByteRangeBufferLength(StreamDetail stream, ref byte[] targetBuffer)
        {
            if (stream == null)
                return;

            if (stream.Range.Length > targetBuffer.Length)
                targetBuffer = new byte[stream.Range.Length];
        }

        private protected async Task GetByteRangeAsync(StreamDetail stream, Memory<byte> outputBuffer)
        {
            if (stream == null)
                return;

            // If current and last ranges are equal, the previous data will be buffered
            // and we can return only the length, without requesting the bytes again.

            if (stream.Range == _lastRange)
                return;

            await _byteRangeProvider.FillBufferAsync(outputBuffer[..stream.Range.Length], stream.Range.Offset);

            _lastRange = stream.Range;
        }

        private protected void DecompressByteRange(StreamDetail stream, ReadOnlySpan<byte> compressedInput, ref byte[] decompressedOutput, ref int decompressedLength)
        {
            decompressedLength = 0;

            if (stream != null)
            {
                decompressedOutput = CompressedData.CheckDecompressionBuffer(compressedInput[..stream.Range.Length], decompressedOutput, _orcFileProperties.CompressionKind, _orcFileProperties.DecompressedChunkMaxLength);
                decompressedLength = CompressedData.Decompress(compressedInput[..stream.Range.Length], decompressedOutput, _orcFileProperties.CompressionKind);
            }
        }

        /// <summary>
        /// Applies the offset position into the decompressed data.
        /// </summary>
        private static protected ReadOnlySpan<byte> GetDataStream(StreamDetail stream, ReadOnlySpan<byte> decompressedBuffer)
        {
            var rowEntryLength = decompressedBuffer.Length - stream.Positions.RowEntryOffset;

            return decompressedBuffer.Slice(stream.Positions.RowEntryOffset, rowEntryLength);
        }

        private static protected double VarIntToDouble(long numerator, long scale)
            => (double)VarIntToDecimal(numerator, scale);

        private static protected decimal VarIntToDecimal(long numerator, long scale)
        {
            if (scale < 0 || scale > 255)
                throw new OverflowException("Scale must be positive number");

            var decNumerator = (decimal)numerator;
            var scaler = new decimal(1, 0, 0, false, (byte)scale);

            return decNumerator * scaler;
        }

        private static long? ReadVarInt(ref BufferReader stream)
        {
            long value = 0;
            long currentLong = 0;
            int bitCount = 0;

            while (true)
            {
                if (!stream.TryRead(out var currentByte))
                    return null;

                currentLong |= ((long)currentByte & 0x7f) << bitCount % 63;
                bitCount += 7;

                if (bitCount % 63 == 0)
                {
                    if (bitCount == 63)
                        value = currentLong;
                    else
                        value |= currentLong << bitCount - 63;

                    currentLong = 0;
                }

                // Done when the high bit is cleared
                if (currentByte < 0x80)
                    break;
            }

            if (currentLong != 0) // Some bits left to add to result
            {
                var shift = bitCount / 63 * 63;
                value |= currentLong << shift;
            }

            // Un zig-zag
            return value.ZigzagDecode();
        }
    }
}
