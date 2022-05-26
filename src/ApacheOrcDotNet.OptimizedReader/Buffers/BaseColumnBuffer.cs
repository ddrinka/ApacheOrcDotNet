using ApacheOrcDotNet.Encodings;
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

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    [SkipLocalsInit]
    public abstract class BaseColumnBuffer<TOutput>
    {
        private protected readonly ArrayPool<byte> _arrayPool;
        private protected readonly IByteRangeProvider _byteRangeProvider;
        private protected readonly OrcContext _context;
        private protected readonly OrcColumn _column;
        private protected readonly TOutput[] _values;
        private protected int _numValuesRead;
        private long[] _numericStreamBuffer;
        private byte[] _byteStreamBuffer;
        private byte[] _boolStreamBuffer;

        public BaseColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContext context, OrcColumn column)
        {
            _byteRangeProvider = byteRangeProvider;
            _context = context;
            _column = column;
            _values = new TOutput[_context.MaxValuesToRead];

            _numericStreamBuffer = new long[1000];
            _byteStreamBuffer = new byte[1000];
            _boolStreamBuffer = new byte[1000];

            _arrayPool = ArrayPool<byte>.Create(15 * 1024 * 1024, 8);
        }

        public OrcColumn Column => _column;
        public ReadOnlySpan<TOutput> Values => _values.AsSpan().Slice(0, _numValuesRead);

        public abstract void Fill(int stripeId, IEnumerable<StreamDetail> columnStreams, RowIndexEntry rowIndexEntry);

        public void Reset() => _numValuesRead = 0;

        private protected int ReadByteStream(ReadOnlySpan<byte> buffer, int length, in DataPositions positions, Span<byte> outputValues, ref int numValuesRead)
        {
            numValuesRead = 0;

            var numSkipped = 0;
            var bufferReader = new BufferReader(ResizeBuffer(buffer, length, in positions));

            while (!bufferReader.Complete)
            {
                var numByteValuesRead = OptimizedByteRLE.ReadValues(ref bufferReader, _byteStreamBuffer);

                for (int idx = 0; idx < numByteValuesRead; idx++)
                {
                    if (numSkipped++ < positions.ValuesToSkip)
                        continue;

                    outputValues[numValuesRead++] = _byteStreamBuffer[idx];

                    if (numValuesRead >= outputValues.Length)
                        return numValuesRead;
                }
            }

            return numValuesRead;
        }

        private protected void ReadBooleanStream(ReadOnlySpan<byte> buffer, int length, in DataPositions positions, Span<bool> outputValues, ref int numValuesRead)
        {
            numValuesRead = 0;

            var numSkipped = 0;
            var bufferReader = new BufferReader(ResizeBuffer(buffer, length, in positions));
            var numOfTotalBitsToSkip = positions.ValuesToSkip * 8 + positions.RemainingBits;
            var numOfBytesToSkip = numOfTotalBitsToSkip / 8;
            while (!bufferReader.Complete)
            {
                var numByteValuesRead = OptimizedByteRLE.ReadValues(ref bufferReader, _boolStreamBuffer);

                for (int idx = 0; idx < numByteValuesRead; idx++)
                {
                    if (numSkipped++ < numOfBytesToSkip)
                        continue;

                    var decodedByte = _boolStreamBuffer[idx];

                    // Skip remaining bits.
                    if (numOfBytesToSkip % 8 != 0)
                        decodedByte = (byte)(decodedByte << numOfTotalBitsToSkip % 8);

                    outputValues[numValuesRead++] = (decodedByte & 128) != 0;
                    if (numValuesRead >= outputValues.Length)
                        return;

                    outputValues[numValuesRead++] = (decodedByte & 64) != 0;
                    if (numValuesRead >= outputValues.Length)
                        return;

                    outputValues[numValuesRead++] = (decodedByte & 32) != 0;
                    if (numValuesRead >= outputValues.Length)
                        return;

                    outputValues[numValuesRead++] = (decodedByte & 16) != 0;
                    if (numValuesRead >= outputValues.Length)
                        return;

                    outputValues[numValuesRead++] = (decodedByte & 8) != 0;
                    if (numValuesRead >= outputValues.Length)
                        return;

                    outputValues[numValuesRead++] = (decodedByte & 4) != 0;
                    if (numValuesRead >= outputValues.Length)
                        return;

                    outputValues[numValuesRead++] = (decodedByte & 2) != 0;
                    if (numValuesRead >= outputValues.Length)
                        return;

                    outputValues[numValuesRead++] = (decodedByte & 1) != 0;
                    if (numValuesRead >= outputValues.Length)
                        return;
                }
            }
        }

        private protected void ReadNumericStream(ReadOnlySpan<byte> buffer, int length, in DataPositions positions, bool isSigned, Span<long> outputValues, ref int numValuesRead)
        {
            numValuesRead = 0;

            var numSkipped = 0;
            var bufferReader = new BufferReader(ResizeBuffer(buffer, length, in positions));

            while (!bufferReader.Complete)
            {
                var numNewValuesRead = OptimizedIntegerRLE.ReadValues(ref bufferReader, isSigned, _numericStreamBuffer);

                for (int idx = 0; idx < numNewValuesRead; idx++)
                {
                    if (numSkipped++ < positions.ValuesToSkip)
                        continue;

                    outputValues[numValuesRead++] = (int)_numericStreamBuffer[idx];

                    if (numValuesRead >= outputValues.Length)
                        return;
                }
            }
        }

        private protected void ReadVarIntStream(ReadOnlySpan<byte> buffer, int length, in DataPositions positions, Span<BigInteger> outputValues, ref int numValuesRead)
        {
            numValuesRead = 0;

            int numSkipped = 0;
            var bufferReader = new BufferReader(ResizeBuffer(buffer, length, in positions));

            while (!bufferReader.Complete)
            {
                var bigInt = ReadBigVarInt(ref bufferReader);

                if (!bigInt.HasValue)
                    return;

                if (numSkipped++ < positions.ValuesToSkip)
                    continue;

                outputValues[numValuesRead++] = bigInt.Value;

                if (numValuesRead >= outputValues.Length)
                    return;
            }
        }

        private protected StreamDetail GetColumnStream(IEnumerable<StreamDetail> columnStreams, StreamKind streamKind, bool isRequired = true)
        {
            var stream = columnStreams.SingleOrDefault(stream =>
                stream.StreamKind == streamKind
            );

            if (isRequired && stream == null)
                throw new InvalidDataException($"The '{streamKind}' stream must be available");

            return stream;
        }

        private protected DataPositions GetPresentStreamPositions(StreamDetail presentStream, RowIndexEntry rowIndexEntry)
        {
            if (presentStream == null)
                return new();

            return new((int)rowIndexEntry.Positions[0], (int)rowIndexEntry.Positions[1], (int)rowIndexEntry.Positions[2], (int)rowIndexEntry.Positions[3]);
        }

        private protected DataPositions GetTargetDataStreamPositions(StreamDetail presentStream, StreamDetail targetedStream, RowIndexEntry rowIndexEntry)
        {
            var positionStep = presentStream == null ? 0 : 4;

            ulong rowGroupOffset = (targetedStream.StreamKind, _column.Type, targetedStream.EncodingKind) switch
            {
                (StreamKind.DictionaryData, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => rowIndexEntry.Positions[positionStep + 0],

                (StreamKind.Secondary, ColumnTypeKind.Timestamp, _) => rowIndexEntry.Positions[positionStep + 3],
                (StreamKind.Secondary, ColumnTypeKind.Decimal, _) => rowIndexEntry.Positions[positionStep + 2],

                (StreamKind.Length, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => rowIndexEntry.Positions[positionStep + 0],
                (StreamKind.Length, ColumnTypeKind.String, ColumnEncodingKind.DirectV2) => rowIndexEntry.Positions[positionStep + 2],
                (StreamKind.Length, ColumnTypeKind.Binary, ColumnEncodingKind.DirectV2) => rowIndexEntry.Positions[positionStep + 2],

                (StreamKind.Data, ColumnTypeKind.Timestamp, _) => rowIndexEntry.Positions[positionStep + 0],
                (StreamKind.Data, ColumnTypeKind.Decimal, _) => rowIndexEntry.Positions[positionStep + 0],
                (StreamKind.Data, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => rowIndexEntry.Positions[positionStep + 0],
                (StreamKind.Data, ColumnTypeKind.String, ColumnEncodingKind.DirectV2) => rowIndexEntry.Positions[positionStep + 0],
                (StreamKind.Data, ColumnTypeKind.Binary, ColumnEncodingKind.DirectV2) => rowIndexEntry.Positions[positionStep + 0],
                (StreamKind.Data, ColumnTypeKind.Short, _) => rowIndexEntry.Positions[positionStep + 0],
                (StreamKind.Data, ColumnTypeKind.Float, _) => rowIndexEntry.Positions[positionStep + 0],
                (StreamKind.Data, ColumnTypeKind.Double, _) => rowIndexEntry.Positions[positionStep + 0],
                (StreamKind.Data, ColumnTypeKind.Date, _) => rowIndexEntry.Positions[positionStep + 0],
                (StreamKind.Data, ColumnTypeKind.Long, _) => rowIndexEntry.Positions[positionStep + 0],
                (StreamKind.Data, ColumnTypeKind.Int, _) => rowIndexEntry.Positions[positionStep + 0],
                (StreamKind.Data, ColumnTypeKind.Byte, _) => rowIndexEntry.Positions[positionStep + 0],

                _ => throw new NotImplementedException()
            };

            ulong rowEntryOffset = (targetedStream.StreamKind, _column.Type, targetedStream.EncodingKind) switch
            {
                (StreamKind.DictionaryData, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => 0,

                (StreamKind.Secondary, ColumnTypeKind.Timestamp, _) => rowIndexEntry.Positions[positionStep + 4],
                (StreamKind.Secondary, ColumnTypeKind.Decimal, _) => rowIndexEntry.Positions[positionStep + 3],

                (StreamKind.Length, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => 0,
                (StreamKind.Length, ColumnTypeKind.String, ColumnEncodingKind.DirectV2) => rowIndexEntry.Positions[positionStep + 3],
                (StreamKind.Length, ColumnTypeKind.Binary, ColumnEncodingKind.DirectV2) => rowIndexEntry.Positions[positionStep + 3],

                (StreamKind.Data, ColumnTypeKind.Decimal, _) => rowIndexEntry.Positions[positionStep + 1],
                (StreamKind.Data, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => rowIndexEntry.Positions[positionStep + 1],
                (StreamKind.Data, ColumnTypeKind.String, ColumnEncodingKind.DirectV2) => rowIndexEntry.Positions[positionStep + 1],
                (StreamKind.Data, ColumnTypeKind.Binary, ColumnEncodingKind.DirectV2) => rowIndexEntry.Positions[positionStep + 1],
                (StreamKind.Data, ColumnTypeKind.Timestamp, _) => rowIndexEntry.Positions[positionStep + 1],
                (StreamKind.Data, ColumnTypeKind.Short, _) => rowIndexEntry.Positions[positionStep + 1],
                (StreamKind.Data, ColumnTypeKind.Double, _) => rowIndexEntry.Positions[positionStep + 1],
                (StreamKind.Data, ColumnTypeKind.Float, _) => rowIndexEntry.Positions[positionStep + 1],
                (StreamKind.Data, ColumnTypeKind.Date, _) => rowIndexEntry.Positions[positionStep + 1],
                (StreamKind.Data, ColumnTypeKind.Long, _) => rowIndexEntry.Positions[positionStep + 1],
                (StreamKind.Data, ColumnTypeKind.Int, _) => rowIndexEntry.Positions[positionStep + 1],
                (StreamKind.Data, ColumnTypeKind.Byte, _) => rowIndexEntry.Positions[positionStep + 1],

                _ => throw new NotImplementedException()
            };

            ulong valuesToSkip = (targetedStream.StreamKind, _column.Type, targetedStream.EncodingKind) switch
            {
                (StreamKind.DictionaryData, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => 0,

                (StreamKind.Secondary, ColumnTypeKind.Timestamp, _) => rowIndexEntry.Positions[positionStep + 5],
                (StreamKind.Secondary, ColumnTypeKind.Decimal, _) => rowIndexEntry.Positions[positionStep + 4],

                (StreamKind.Length, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => 0,
                (StreamKind.Length, ColumnTypeKind.String, ColumnEncodingKind.DirectV2) => rowIndexEntry.Positions[positionStep + 4],
                (StreamKind.Length, ColumnTypeKind.Binary, ColumnEncodingKind.DirectV2) => rowIndexEntry.Positions[positionStep + 4],

                (StreamKind.Data, ColumnTypeKind.Timestamp, _) => rowIndexEntry.Positions[positionStep + 2],
                (StreamKind.Data, ColumnTypeKind.Decimal, _) => 0,
                (StreamKind.Data, ColumnTypeKind.Double, _) => 0,
                (StreamKind.Data, ColumnTypeKind.Float, _) => 0,
                (StreamKind.Data, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => rowIndexEntry.Positions[positionStep + 2],
                (StreamKind.Data, ColumnTypeKind.String, ColumnEncodingKind.DirectV2) => 0,
                (StreamKind.Data, ColumnTypeKind.Binary, ColumnEncodingKind.DirectV2) => 0,
                (StreamKind.Data, ColumnTypeKind.Short, _) => rowIndexEntry.Positions[positionStep + 2],
                (StreamKind.Data, ColumnTypeKind.Date, _) => rowIndexEntry.Positions[positionStep + 2],
                (StreamKind.Data, ColumnTypeKind.Long, _) => rowIndexEntry.Positions[positionStep + 2],
                (StreamKind.Data, ColumnTypeKind.Int, _) => rowIndexEntry.Positions[positionStep + 2],
                (StreamKind.Data, ColumnTypeKind.Byte, _) => rowIndexEntry.Positions[positionStep + 2],

                _ => throw new NotImplementedException()
            };

            return new((int)rowGroupOffset, (int)rowEntryOffset, (int)valuesToSkip);
        }

        private protected void GetByteRange(Span<byte> output, StreamDetail stream, in DataPositions positions, ref int rangeLength)
        {
            rangeLength = 0;

            if (stream != null)
            {
                var offset = stream.FileOffset + positions.RowGroupOffset;
                var compressedLength = stream.Length - positions.RowGroupOffset;

                rangeLength = _byteRangeProvider.GetRange(output.Slice(0, compressedLength), offset);
            }
        }

        private protected void DecompressByteRange(ReadOnlySpan<byte> compressedInput, Span<byte> decompressedOutput, StreamDetail stream, in DataPositions positions, ref int decompressedLength)
        {
            decompressedLength = 0;

            if (stream != null)
            {
                var compressedLength = stream.Length - positions.RowGroupOffset;

                decompressedLength = StreamData.Decompress(compressedInput.Slice(0, compressedLength), decompressedOutput, _context.CompressionKind);
            }
        }

        private protected ReadOnlySpan<byte> ResizeBuffer(ReadOnlySpan<byte> buffer, int length, in DataPositions positions)
        {
            var rowentrylength = length - positions.RowEntryOffset;

            return buffer.Slice(positions.RowEntryOffset, rowentrylength);
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
