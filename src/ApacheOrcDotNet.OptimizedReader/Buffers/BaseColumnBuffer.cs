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
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    [SkipLocalsInit]
    public abstract class BaseColumnBuffer<TOutput>
    {
        //private protected readonly BufferStream[] _bufferStreams;
        private protected readonly IByteRangeProvider _byteRangeProvider;
        private protected readonly OrcContext _context;
        private protected readonly OrcColumn _column;
        private protected readonly TOutput[] _values;

        private protected readonly ArrayPool<byte> _pool;

        private protected StreamDetails _dataStream;
        private protected StreamPositions _dataStreamPositions;
        private protected byte[] _dataStreamCompressedBuffer;
        private protected byte[] _dataStreamDecompressedBuffer;
        private protected int _dataStreamDecompressedBufferLength;

        private protected StreamDetails _dictionaryStream;
        private protected StreamPositions _dictionaryStreamPositions;
        private protected byte[] _dictionaryStreanCompressedBuffer;
        private protected byte[] _dictionaryStreamDecompressedBuffer;
        private protected int _dictionaryStreamDecompressedBufferLength;

        private protected StreamDetails _lengthStream;
        private protected StreamPositions _lengthStreamPositions;
        private protected byte[] _lengthStreamCompressedBuffer;
        private protected byte[] _lengthStreamDecompressedBuffer;
        private protected int _lengthStreamDecompressedBufferLength;

        private protected StreamDetails _presentStream;
        private protected StreamPositions _presentStreamPositions;
        private protected byte[] _presentStreamCompressedBuffer;
        private protected byte[] _presentStreamDecompressedBuffer;
        private protected int _presentStreamDecompressedBufferLength;

        private protected StreamDetails _secondaryStream;
        private protected StreamPositions _secondaryStreamPositions;
        private protected byte[] _secondaryStreamCompressedBuffer;
        private protected byte[] _secondaryStreamDecompressedBuffer;
        private protected int _secondaryStreamDecompressedBufferLength;

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

            _pool = ArrayPool<byte>.Create(15 * 1024 * 1024, 8);

            _dataStreamCompressedBuffer = _pool.Rent(_context.MaxCompressedBufferLength);
            _dataStreamDecompressedBuffer = _pool.Rent(_context.MaxDecompresseBufferLength);

            _dictionaryStreanCompressedBuffer = _pool.Rent(_context.MaxCompressedBufferLength);
            _dictionaryStreamDecompressedBuffer = _pool.Rent(_context.MaxDecompresseBufferLength);

            _lengthStreamCompressedBuffer = _pool.Rent(_context.MaxCompressedBufferLength);
            _lengthStreamDecompressedBuffer = _pool.Rent(_context.MaxDecompresseBufferLength);

            _presentStreamCompressedBuffer = _pool.Rent(_context.MaxCompressedBufferLength);
            _presentStreamDecompressedBuffer = _pool.Rent(_context.MaxDecompresseBufferLength);

            _secondaryStreamCompressedBuffer = _pool.Rent(_context.MaxCompressedBufferLength);
            _secondaryStreamDecompressedBuffer = _pool.Rent(_context.MaxDecompresseBufferLength);

            //var streamKinds = Enum.GetValues<StreamKind>();
            //_bufferStreams = new BufferStream[streamKinds.Length];

            //foreach (var kind in streamKinds)
            //{
            //    _bufferStreams[(int)kind] = new BufferStream()
            //    {
            //        CompressedBuffer = _pool.Rent(_context.MaxCompressedBufferLength),
            //        DecompressedBuffer = _pool.Rent(_context.MaxDecompresseBufferLength)
            //    };
            //}

            _numericStreamBuffer = new long[1000];
            _byteStreamBuffer = new byte[1000];
            _boolStreamBuffer = new byte[1000];
        }

        public OrcColumn Column => _column;
        public ReadOnlySpan<TOutput> Values => _values.AsSpan().Slice(0, _numValuesRead);

        public abstract Task LoadDataAsync(int stripeId, IEnumerable<StreamDetails> columnStreams, RowIndexEntry rowIndexEntry);
        public abstract void Parse();

        public void Reset() => _numValuesRead = 0;

        private protected int ReadByteStream(ReadOnlySpan<byte> buffer, int length, in StreamPositions positions, Span<byte> outputValues, out int numValuesRead)
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

        private protected void ReadBooleanStream(ReadOnlySpan<byte> buffer, int length, in StreamPositions positions, Span<bool> outputValues, out int numValuesRead)
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

        private protected void ReadNumericStream(ReadOnlySpan<byte> buffer, int length, in StreamPositions positions, bool isSigned, Span<long> outputValues, out int numValuesRead)
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

        private protected void ReadVarIntStream(ReadOnlySpan<byte> buffer, int length, in StreamPositions positions, Span<BigInteger> outputValues, out int numValuesRead)
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

        private protected void LoadStreams(IEnumerable<StreamDetails> columnStreams, RowIndexEntry rowIndexEntry, params StreamKind[] requiredStreams)
        {
            var missingStreams = requiredStreams.ToList();

            _presentStream = columnStreams.SingleOrDefault(stream => stream.StreamKind == StreamKind.Present);
            _presentStreamPositions = GetPresentStreamPositions(_presentStream, rowIndexEntry);
            missingStreams.Remove(StreamKind.Present);

            foreach (var stream in columnStreams)
            {
                switch (stream.StreamKind)
                {
                    case StreamKind.Present:
                    case StreamKind.RowIndex:
                        continue;
                    case StreamKind.Secondary:
                        _secondaryStream = stream;
                        _secondaryStreamPositions = GetRequiredStreamPositions(stream, rowIndexEntry);
                        missingStreams.Remove(stream.StreamKind);
                        break;
                    case StreamKind.DictionaryData:
                        _dictionaryStream = stream;
                        _dictionaryStreamPositions = GetRequiredStreamPositions(stream, rowIndexEntry);
                        missingStreams.Remove(stream.StreamKind);
                        break;
                    case StreamKind.Length:
                        _lengthStream = stream;
                        _lengthStreamPositions = GetRequiredStreamPositions(stream, rowIndexEntry);
                        missingStreams.Remove(stream.StreamKind);
                        break;
                    case StreamKind.Data:
                        _dataStream = stream;
                        _dataStreamPositions = GetRequiredStreamPositions(stream, rowIndexEntry);
                        missingStreams.Remove(stream.StreamKind);
                        break;
                    default:
                        throw new NotImplementedException($"Unable to set stream '{stream.StreamKind}'");
                }
            }

            if (missingStreams.Count > 0)
                throw new InvalidDataException($"The following streams must be available: {string.Join(", ", missingStreams)}");
        }

        private protected async Task<int> GetByteRange(Memory<byte> output, StreamDetails stream, StreamPositions positions)
        {
            var rangeLength = 0;

            if (stream != null)
            {
                var offset = stream.FileOffset + positions.RowGroupOffset;
                var compressedLength = stream.Length - positions.RowGroupOffset;

                //Console.WriteLine($"GetByteRangeAsync for '{_column.Name}': {compressedLength}/{offset}");

                rangeLength = await _byteRangeProvider.GetRangeAsync(output.Slice(0, compressedLength), offset);
            }

            return rangeLength;
        }

        private protected void DecompressByteRange(ReadOnlySpan<byte> compressedInput, Span<byte> decompressedOutput, StreamDetails stream, in StreamPositions positions, ref int decompressedLength)
        {
            decompressedLength = 0;

            if (stream != null)
            {
                var compressedLength = stream.Length - positions.RowGroupOffset;

                decompressedLength = StreamData.Decompress(compressedInput.Slice(0, compressedLength), decompressedOutput, _context.CompressionKind);
            }
        }

        private protected ReadOnlySpan<byte> ResizeBuffer(ReadOnlySpan<byte> buffer, int length, in StreamPositions positions)
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

        private StreamPositions GetPresentStreamPositions(StreamDetails presentStream, RowIndexEntry rowIndexEntry)
        {
            if (presentStream == null)
                return new();

            return new((int)rowIndexEntry.Positions[0], (int)rowIndexEntry.Positions[1], (int)rowIndexEntry.Positions[2], (int)rowIndexEntry.Positions[3]);
        }

        private StreamPositions GetRequiredStreamPositions(StreamDetails targetedStream, RowIndexEntry rowIndexEntry)
        {
            var positionStep = _presentStream == null ? 0 : 4;

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
                (StreamKind.Data, ColumnTypeKind.Boolean, _) => rowIndexEntry.Positions[positionStep + 0],

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
                (StreamKind.Data, ColumnTypeKind.Boolean, _) => rowIndexEntry.Positions[positionStep + 1],

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
                (StreamKind.Data, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => rowIndexEntry.Positions[positionStep + 2],
                (StreamKind.Data, ColumnTypeKind.String, ColumnEncodingKind.DirectV2) => 0,
                (StreamKind.Data, ColumnTypeKind.Binary, ColumnEncodingKind.DirectV2) => 0,
                (StreamKind.Data, ColumnTypeKind.Short, _) => rowIndexEntry.Positions[positionStep + 2],
                (StreamKind.Data, ColumnTypeKind.Double, _) => 0,
                (StreamKind.Data, ColumnTypeKind.Float, _) => 0,
                (StreamKind.Data, ColumnTypeKind.Date, _) => rowIndexEntry.Positions[positionStep + 2],
                (StreamKind.Data, ColumnTypeKind.Long, _) => rowIndexEntry.Positions[positionStep + 2],
                (StreamKind.Data, ColumnTypeKind.Int, _) => rowIndexEntry.Positions[positionStep + 2],
                (StreamKind.Data, ColumnTypeKind.Byte, _) => rowIndexEntry.Positions[positionStep + 2],
                (StreamKind.Data, ColumnTypeKind.Boolean, _) => rowIndexEntry.Positions[positionStep + 2],

                _ => throw new NotImplementedException()
            };

            ulong remainingBits = (targetedStream.StreamKind, _column.Type, targetedStream.EncodingKind) switch
            {
                (StreamKind.Data, ColumnTypeKind.Boolean, _) => rowIndexEntry.Positions[positionStep + 3],
                _ => 0
            };

            return new((int)rowGroupOffset, (int)rowEntryOffset, (int)valuesToSkip, (int)remainingBits);
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
