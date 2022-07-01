﻿using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.OptimizedReader.Encodings;
using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    [SkipLocalsInit]
    public abstract class BaseColumnBuffer<TOutput>
    {
        private readonly long[] _numericStreamBuffer;
        private readonly byte[] _byteStreamBuffer;
        private readonly byte[] _boolStreamBuffer;

        private protected readonly IByteRangeProvider _byteRangeProvider;
        private protected readonly OrcFileProperties _context;
        private protected readonly OrcColumn _column;
        private protected readonly TOutput[] _values;

        private protected readonly ArrayPool<byte> _pool;

        private protected int _numValuesRead;

        private StreamRange _lastRange;
        private int _lastRangeLength;

        public BaseColumnBuffer(IByteRangeProvider byteRangeProvider, OrcFileProperties context, OrcColumn column)
        {
            _byteRangeProvider = byteRangeProvider;
            _context = context;
            _column = column;
            _values = new TOutput[_context.MaxValuesToRead];

            _pool = ArrayPool<byte>.Create(15 * 1024 * 1024, 8);

            _numericStreamBuffer = new long[1000];
            _byteStreamBuffer = new byte[1000];
            _boolStreamBuffer = new byte[1000];
        }

        public OrcColumn Column => _column;
        public ReadOnlySpan<TOutput> Values => _values.AsSpan().Slice(0, _numValuesRead);

        public abstract Task LoadDataAsync(int stripeId, ColumnDataStreams streams);
        public abstract void Fill();

        public void Reset() => _numValuesRead = 0;

        private protected StreamDetail GetStripeStream(IEnumerable<StreamDetail> columnStreams, StreamKind streamKind, bool isRequired = true)
        {
            var stream = columnStreams.SingleOrDefault(stream =>
                stream.StreamKind == streamKind
            );

            if (isRequired && stream == null)
                throw new InvalidDataException($"The '{streamKind}' stream must be available");

            return stream;
        }

        private protected void ReadByteStream(StreamDetail stream, ReadOnlySpan<byte> buffer, int length, Span<byte> outputValues, out int numValuesRead)
        {
            numValuesRead = 0;

            if (stream == null)
                return;

            var numSkipped = 0;
            var bufferReader = new BufferReader(GetDataStream(stream, buffer, length));

            while (!bufferReader.Complete)
            {
                var numByteValuesRead = OptimizedByteRLE.ReadValues(ref bufferReader, _byteStreamBuffer);

                for (int idx = 0; idx < numByteValuesRead; idx++)
                {
                    if (numSkipped++ < stream.Positions.ValuesToSkip)
                        continue;

                    outputValues[numValuesRead++] = _byteStreamBuffer[idx];

                    if (numValuesRead >= outputValues.Length)
                        return;
                }
            }
        }

        private protected void ReadBooleanStream(StreamDetail stream, ReadOnlySpan<byte> buffer, int length, Span<bool> outputValues, out int numValuesRead)
        {
            numValuesRead = 0;

            if (stream == null)
                return;

            var numSkipped = 0;
            var bufferReader = new BufferReader(GetDataStream(stream, buffer, length));
            var numOfTotalBitsToSkip = stream.Positions.ValuesToSkip * 8 + stream.Positions.RemainingBits;
            var numOfBytesToSkip = numOfTotalBitsToSkip / 8;
            while (!bufferReader.Complete)
            {
                var numByteValuesRead = OptimizedByteRLE.ReadValues(ref bufferReader, _boolStreamBuffer);

                for (int idx = 0; idx < numByteValuesRead; idx++)
                {
                    if (numSkipped++ < numOfBytesToSkip)
                        continue;

                    var decodedByte = _boolStreamBuffer[idx];
                    var isFinalByte = bufferReader.Complete && idx >= numByteValuesRead - 1;

                    // Skip remaining bits.
                    if (numOfBytesToSkip % 8 != 0)
                        decodedByte = (byte)(decodedByte << numOfTotalBitsToSkip % 8);

                    if (isFinalByte && decodedByte == 0)
                    {
                        // Edge case where there is only one value for the row entry and that value is null
                        outputValues[numValuesRead++] = false;
                        return;
                    }

                    for (int bitIdx = 7; bitIdx >= 0; bitIdx--)
                    {
                        outputValues[numValuesRead++] = (decodedByte & 1 << bitIdx) != 0;

                        if (numValuesRead >= outputValues.Length)
                            return;

                        if (isFinalByte && BitOperations.TrailingZeroCount(decodedByte) == bitIdx)
                            return;
                    }
                }
            }
        }

        private protected void ReadNumericStream(StreamDetail stream, ReadOnlySpan<byte> buffer, int length, bool isSigned, Span<long> outputValues, out int numValuesRead)
        {
            numValuesRead = 0;

            if (stream == null)
                return;

            var numSkipped = 0;
            var bufferReader = new BufferReader(GetDataStream(stream, buffer, length));

            while (!bufferReader.Complete)
            {
                var numNewValuesRead = OptimizedIntegerRLE.ReadValues(ref bufferReader, isSigned, _numericStreamBuffer);

                for (int idx = 0; idx < numNewValuesRead; idx++)
                {
                    if (numSkipped++ < stream.Positions.ValuesToSkip)
                        continue;

                    outputValues[numValuesRead++] = (int)_numericStreamBuffer[idx];

                    if (numValuesRead >= outputValues.Length)
                        return;
                }
            }
        }

        private protected void ReadVarIntStream(StreamDetail stream, ReadOnlySpan<byte> buffer, int length, Span<BigInteger> outputValues, out int numValuesRead)
        {
            numValuesRead = 0;

            if (stream == null)
                return;

            int numSkipped = 0;
            var bufferReader = new BufferReader(GetDataStream(stream, buffer, length));

            while (!bufferReader.Complete)
            {
                var bigInt = ReadBigVarInt(ref bufferReader);

                if (!bigInt.HasValue)
                    return;

                if (numSkipped++ < stream.Positions.ValuesToSkip)
                    continue;

                outputValues[numValuesRead++] = bigInt.Value;

                if (numValuesRead >= outputValues.Length)
                    return;
            }
        }

        private protected async Task<int> GetByteRangeAsync(StreamDetail stream, Memory<byte> output)
        {
            if (stream != null)
            {
                // If last range matches, the previous data will already be buffered.
                // We can return only the length without requesting the same bytes.

                if (stream.Range != _lastRange)
                    _lastRangeLength = await _byteRangeProvider.GetRangeAsync(output.Slice(0, stream.Range.Length), stream.Range.Offset);

                _lastRange = stream.Range;
            }

            return _lastRangeLength;
        }

        private protected void DecompressByteRange(StreamDetail stream, ReadOnlySpan<byte> compressedInput, Span<byte> decompressedOutput, ref int decompressedLength)
        {
            decompressedLength = 0;

            if (stream != null)
                decompressedLength = StreamData.Decompress(compressedInput.Slice(0, stream.Range.Length), decompressedOutput, _context.CompressionKind);
        }

        /// <summary>
        /// Applies the offset position into the decompressed data.
        /// </summary>
        private protected ReadOnlySpan<byte> GetDataStream(StreamDetail stream, ReadOnlySpan<byte> decompressedBuffer, int decompressedBufferLength)
        {
            var rowEntryLength = decompressedBufferLength - stream.Positions.RowEntryOffset;

            return decompressedBuffer.Slice(stream.Positions.RowEntryOffset, rowEntryLength);
        }

        private protected double BigIntegerToDouble(BigInteger numerator, long scale)
            => (double)BigIntegerToDecimal(numerator, scale);

        private protected decimal BigIntegerToDecimal(BigInteger numerator, long scale)
        {
            if (scale < 0 || scale > 255)
                throw new OverflowException("Scale must be positive number");

            var decNumerator = (decimal)numerator; //This will throw for an overflow or underflow
            var scaler = new decimal(1, 0, 0, false, (byte)scale);

            return decNumerator * scaler;
        }

        private BigInteger? ReadBigVarInt(ref BufferReader stream)
        {
            var value = BigInteger.Zero;
            long currentLong = 0;
            int bitCount = 0;

            while (true)
            {
                if (!stream.TryRead(out var currentByte))
                    return null; // Reached the end of the stream

                currentLong |= ((long)currentByte & 0x7f) << bitCount % 63;
                bitCount += 7;

                if (bitCount % 63 == 0)
                {
                    if (bitCount == 63)
                        value = new BigInteger(currentLong);
                    else
                        value |= new BigInteger(currentLong) << bitCount - 63;

                    currentLong = 0;
                }

                // Done when the high bit is set
                if (currentByte < 0x80)
                    break;
            }

            if (currentLong != 0) // Some bits left to add to result
            {
                var shift = bitCount / 63 * 63;
                value |= new BigInteger(currentLong) << shift;
            }

            // Un zig-zag
            return ((long)value).ZigzagDecode();
        }
    }
}
