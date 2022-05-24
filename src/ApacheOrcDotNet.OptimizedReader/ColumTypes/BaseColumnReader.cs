using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.OptimizedReader.Encodings;
using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System;
using System.Buffers;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace ApacheOrcDotNet.OptimizedReader.ColumTypes
{
    public readonly record struct StreamPositions(int RowGroupOffset = 0, int RowEntryOffset = 0, int ValuesToSkip = 0, int RemainingBits = 0);

    public abstract class BaseColumnReader<TOutput> : IDisposable
    {
        private protected readonly ReaderContext _readerContext;
        private protected readonly TOutput[] _outputValuesRaw;
        private protected readonly int _numMaxValuesToRead;
        private protected int _numValuesRead;

        public BaseColumnReader(ReaderContext readerContext)
        {
            _readerContext = readerContext;
            _outputValuesRaw = ArrayPool<TOutput>.Shared.Rent((int)_readerContext.FileTail.Footer.RowIndexStride);
            //_numMaxValuesToRead = (int)Math.Min(_readerContext.FileTail.Footer.RowIndexStride, _readerContext.RowIndexEntry.Statistics.NumberOfValues);
            _numMaxValuesToRead = (int)_readerContext.FileTail.Footer.RowIndexStride;
        }

        public Span<TOutput> Values => _outputValuesRaw.AsSpan().Slice(0, _numValuesRead);

        public virtual void Dispose()
        {
            ArrayPool<TOutput>.Shared.Return(_outputValuesRaw);
        }

        public abstract void FillBuffer();

        protected StreamDetail GetStripeStream(StreamKind streamKind, bool isRequired = true)
        {
            var stream = _readerContext.Streams.SingleOrDefault(stream =>
                stream.ColumnId == _readerContext.Column.ColumnId
                && stream.StreamKind == streamKind
            );

            if (isRequired && stream == null)
                throw new InvalidDataException($"The '{streamKind}' stream must be available");

            return stream;
        }

        [SkipLocalsInit]
        private protected int ReadByteStream(StreamDetail stream, in StreamPositions positions, Span<byte> outputValues)
        {
            if (stream == null)
                return 0;

            var dataBuffer = _readerContext.ByteRangeProvider.DecompressByteRange(
                offset: stream.FileOffset + positions.RowGroupOffset,
                compressedLength: stream.Length - positions.RowGroupOffset,
                compressionKind: _readerContext.CompressionKind,
                compressionBlockSize: _readerContext.CompressionBlockSize
            );

            var valuesBuffer = ArrayPool<byte>.Shared.Rent(1_000);

            try
            {
                using (dataBuffer)
                {
                    var valuesBufferSpan = valuesBuffer.AsSpan().Slice(0, 1_000);
                    var rowEntryLength = dataBuffer.Sequence.Length - positions.RowEntryOffset;
                    var dataSequence = dataBuffer.Sequence.Slice(positions.RowEntryOffset, rowEntryLength);
                    var dataReader = new SequenceReader<byte>(dataSequence);

                    var numValuesRead = 0;
                    var skippedValues = 0;
                    while (!dataReader.End)
                    {
                        var numByteValuesRead = OptimizedByteRunLengthEncodingReader.ReadValues(
                            ref dataReader,
                            valuesBufferSpan
                        );

                        for (int idx = 0; idx < numByteValuesRead; idx++)
                        {
                            if (skippedValues++ < positions.ValuesToSkip)
                                continue;

                            outputValues[numValuesRead++] = valuesBufferSpan[idx];

                            if (numValuesRead >= outputValues.Length)
                                return numValuesRead;
                        }
                    }

                    return numValuesRead;
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(valuesBuffer);
            }
        }

        [SkipLocalsInit]
        private protected int ReadBooleanStream(StreamDetail stream, in StreamPositions positions, Span<bool> outputValues)
        {
            if (stream == null)
                return 0;

            var dataBuffer = _readerContext.ByteRangeProvider.DecompressByteRange(
                offset: stream.FileOffset + positions.RowGroupOffset,
                compressedLength: stream.Length - positions.RowGroupOffset,
                compressionKind: _readerContext.CompressionKind,
                compressionBlockSize: _readerContext.CompressionBlockSize
            );

            using (dataBuffer)
            {
                var valuesBuffer = ArrayPool<byte>.Shared.Rent(1_000);

                try
                {
                    var valuesBufferSpan = valuesBuffer.AsSpan().Slice(0, 1_000);
                    var rowEntryLength = dataBuffer.Sequence.Length - positions.RowEntryOffset;
                    var dataSequence = dataBuffer.Sequence.Slice(positions.RowEntryOffset, rowEntryLength);
                    var dataReader = new SequenceReader<byte>(dataSequence);

                    var numOfTotalBitsToSkip = (positions.ValuesToSkip * 8) + positions.RemainingBits;
                    var numOfBytesToSkip = numOfTotalBitsToSkip / 8;
                    var numValuesRead = 0;
                    var skippedValues = 0;
                    while (!dataReader.End)
                    {
                        var numByteValuesRead = OptimizedByteRunLengthEncodingReader.ReadValues(
                            ref dataReader,
                            valuesBufferSpan
                        );

                        for (int idx = 0; idx < numByteValuesRead; idx++)
                        {
                            if (skippedValues++ < numOfBytesToSkip)
                                continue;

                            var decodedByte = valuesBufferSpan[idx];

                            // Skip remaining bits.
                            if (numOfBytesToSkip % 8 != 0)
                            {
                                decodedByte = (byte)(decodedByte << (numOfTotalBitsToSkip % 8));
                            }

                            outputValues[numValuesRead++] = (decodedByte & 128) != 0;
                            if (numValuesRead >= outputValues.Length)
                                return numValuesRead;

                            outputValues[numValuesRead++] = (decodedByte & 64) != 0;
                            if (numValuesRead >= outputValues.Length)
                                return numValuesRead;

                            outputValues[numValuesRead++] = (decodedByte & 32) != 0;
                            if (numValuesRead >= outputValues.Length)
                                return numValuesRead;

                            outputValues[numValuesRead++] = (decodedByte & 16) != 0;
                            if (numValuesRead >= outputValues.Length)
                                return numValuesRead;

                            outputValues[numValuesRead++] = (decodedByte & 8) != 0;
                            if (numValuesRead >= outputValues.Length)
                                return numValuesRead;

                            outputValues[numValuesRead++] = (decodedByte & 4) != 0;
                            if (numValuesRead >= outputValues.Length)
                                return numValuesRead;

                            outputValues[numValuesRead++] = (decodedByte & 2) != 0;
                            if (numValuesRead >= outputValues.Length)
                                return numValuesRead;

                            outputValues[numValuesRead++] = (decodedByte & 1) != 0;
                            if (numValuesRead >= outputValues.Length)
                                return numValuesRead;
                        }
                    }

                    /*
                    var numValuesRead = 0;
                    var skippedValues = 0;
                    while (!dataReader.End)
                    {
                        var numByteValuesRead = OptimizedByteRunLengthEncodingReader.ReadValues(
                            ref dataReader,
                            valuesBufferSpan
                        );

                        for (int idx = 0; idx < numByteValuesRead; idx++)
                        {
                            if (skippedValues++ < positions.ValuesToSkip)
                                continue;

                            var decodedByte = valuesBufferSpan[idx];

                            //if (decodedByte <= 0x80)
                            outputValues[numValuesRead++] = (decodedByte & 128) != 0;
                            if (numValuesRead >= outputValues.Length)
                                return numValuesRead;

                            outputValues[numValuesRead++] = (decodedByte & 64) != 0;
                            if (numValuesRead >= outputValues.Length)
                                return numValuesRead;

                            outputValues[numValuesRead++] = (decodedByte & 32) != 0;
                            if (numValuesRead >= outputValues.Length)
                                return numValuesRead;

                            outputValues[numValuesRead++] = (decodedByte & 16) != 0;
                            if (numValuesRead >= outputValues.Length)
                                return numValuesRead;

                            outputValues[numValuesRead++] = (decodedByte & 8) != 0;
                            if (numValuesRead >= outputValues.Length)
                                return numValuesRead;

                            outputValues[numValuesRead++] = (decodedByte & 4) != 0;
                            if (numValuesRead >= outputValues.Length)
                                return numValuesRead;

                            outputValues[numValuesRead++] = (decodedByte & 2) != 0;
                            if (numValuesRead >= outputValues.Length)
                                return numValuesRead;

                            outputValues[numValuesRead++] = (decodedByte & 1) != 0;
                            if (numValuesRead >= outputValues.Length)
                                return numValuesRead;
                        }
                    }
                    */

                    return numValuesRead;
                }
                finally
                {
                    ArrayPool<byte>.Shared.Return(valuesBuffer);
                }
            }
        }

        [SkipLocalsInit]
        private protected int ReadNumericStream(StreamDetail stream, in StreamPositions positions, bool isSigned, Span<long> outputValues)
        {
            if (stream == null)
                return 0;

            if (stream.EncodingKind != ColumnEncodingKind.DirectV2 && stream.EncodingKind != ColumnEncodingKind.DictionaryV2)
                throw new NotImplementedException($"Unimplemented Numeric {nameof(ColumnEncodingKind)} {stream.EncodingKind}");

            var dataBuffer = _readerContext.ByteRangeProvider.DecompressByteRange(
                offset: stream.FileOffset + positions.RowGroupOffset,
                compressedLength: stream.Length - positions.RowGroupOffset,
                compressionKind: _readerContext.CompressionKind,
                compressionBlockSize: _readerContext.CompressionBlockSize
            );

            var valuesBuffer = ArrayPool<long>.Shared.Rent(1_000);

            try
            {
                var valuesBufferSpan = valuesBuffer.AsSpan().Slice(0, 1_000);
                var rowEntryLength = dataBuffer.Sequence.Length - positions.RowEntryOffset;
                var dataSequence = dataBuffer.Sequence.Slice(positions.RowEntryOffset, rowEntryLength);
                var dataReader = new SequenceReader<byte>(dataSequence);

                using (dataBuffer)
                {
                    var numValuesRead = 0;
                    var skippedValues = 0;
                    while (!dataReader.End)
                    {
                        var numNewValuesRead = OptimizedIntegerRunLengthEncodingV2.ReadValues(
                            ref dataReader,
                            isSigned,
                            valuesBufferSpan
                        );

                        for (int idx = 0; idx < numNewValuesRead; idx++)
                        {
                            if (skippedValues++ < positions.ValuesToSkip)
                                continue;

                            outputValues[numValuesRead++] = (int)valuesBuffer[idx];

                            if (numValuesRead >= outputValues.Length)
                                return numValuesRead;
                        }
                    }

                    return numValuesRead;
                }
            }
            finally
            {
                ArrayPool<long>.Shared.Return(valuesBuffer);
            }
        }

        [SkipLocalsInit]
        private protected int ReadVarIntStream(StreamDetail stream, StreamPositions positions, Span<BigInteger> outputValues)
        {
            var numValuesRead = 0;

            if (stream == null)
                return numValuesRead;

            int skippedValues = 0;

            var dataBuffer = _readerContext.ByteRangeProvider.DecompressByteRange(
                offset: stream.FileOffset + positions.RowGroupOffset,
                compressedLength: stream.Length - positions.RowGroupOffset,
                compressionKind: _readerContext.CompressionKind,
                compressionBlockSize: _readerContext.CompressionBlockSize
            );

            using (dataBuffer)
            {
                var rowEntryLength = dataBuffer.Sequence.Length - positions.RowEntryOffset;
                var dataSequence = dataBuffer.Sequence.Slice(positions.RowEntryOffset, rowEntryLength);
                var dataReader = new SequenceReader<byte>(dataSequence);

                while (!dataReader.End)
                {
                    var bigInt = ReadBigVarInt(ref dataReader);

                    if (!bigInt.HasValue)
                        break;

                    if (skippedValues++ < positions.ValuesToSkip)
                        continue;

                    outputValues[numValuesRead++] = bigInt.Value;

                    if (numValuesRead >= outputValues.Length)
                        break;
                }

                return numValuesRead;
            }
        }

        private protected StreamPositions GetPresentStreamPositions(StreamDetail presentStream)
        {
            if (presentStream == null)
                return new();

            return new(GetRowEntryPosition(0), GetRowEntryPosition(1), GetRowEntryPosition(2), GetRowEntryPosition(3));
        }

        private protected StreamPositions GetTargetedStreamPositions(StreamDetail presentStream, StreamDetail targetedStream)
        {
            var positionStep = presentStream == null ? 0 : 4;

            int rowGroupOffset = (targetedStream.StreamKind, _readerContext.Column.ColumnType, targetedStream.EncodingKind) switch
            {
                (StreamKind.DictionaryData, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => GetRowEntryPosition(positionStep + 0),

                (StreamKind.Secondary, ColumnTypeKind.Decimal, _) => GetRowEntryPosition(positionStep + 2),

                (StreamKind.Length, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => GetRowEntryPosition(positionStep + 0),
                (StreamKind.Length, ColumnTypeKind.String, ColumnEncodingKind.DirectV2) => GetRowEntryPosition(positionStep + 2),

                (StreamKind.Data, ColumnTypeKind.Decimal, _) => GetRowEntryPosition(positionStep + 0),
                (StreamKind.Data, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => GetRowEntryPosition(positionStep + 0),
                (StreamKind.Data, ColumnTypeKind.String, ColumnEncodingKind.DirectV2) => GetRowEntryPosition(positionStep + 0),
                (StreamKind.Data, ColumnTypeKind.Short, _) => GetRowEntryPosition(positionStep + 0),
                (StreamKind.Data, ColumnTypeKind.Long, _) => GetRowEntryPosition(positionStep + 0),
                (StreamKind.Data, ColumnTypeKind.Int, _) => GetRowEntryPosition(positionStep + 0),

                _ => GetRowEntryPosition(positionStep + 0)
                //_ => throw new NotImplementedException()
            };
            int rowEntryOffset = (targetedStream.StreamKind, _readerContext.Column.ColumnType, targetedStream.EncodingKind) switch
            {
                (StreamKind.DictionaryData, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => 0,

                (StreamKind.Secondary, ColumnTypeKind.Decimal, _) => GetRowEntryPosition(positionStep + 3),

                (StreamKind.Length, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => 0,
                (StreamKind.Length, ColumnTypeKind.String, ColumnEncodingKind.DirectV2) => GetRowEntryPosition(positionStep + 3),

                (StreamKind.Data, ColumnTypeKind.Decimal, _) => GetRowEntryPosition(positionStep + 1),
                (StreamKind.Data, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => GetRowEntryPosition(positionStep + 1),
                (StreamKind.Data, ColumnTypeKind.String, ColumnEncodingKind.DirectV2) => GetRowEntryPosition(positionStep + 1),
                (StreamKind.Data, ColumnTypeKind.Short, _) => GetRowEntryPosition(positionStep + 1),
                (StreamKind.Data, ColumnTypeKind.Long, _) => GetRowEntryPosition(positionStep + 1),
                (StreamKind.Data, ColumnTypeKind.Int, _) => GetRowEntryPosition(positionStep + 1),

                _ => GetRowEntryPosition(positionStep + 1)
                //_ => throw new NotImplementedException()
            };
            int valuesToSkip = (targetedStream.StreamKind, _readerContext.Column.ColumnType, targetedStream.EncodingKind) switch
            {
                (StreamKind.DictionaryData, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => 0,

                (StreamKind.Secondary, ColumnTypeKind.Decimal, _) => GetRowEntryPosition(positionStep + 4),

                (StreamKind.Length, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => 0,
                (StreamKind.Length, ColumnTypeKind.String, ColumnEncodingKind.DirectV2) => GetRowEntryPosition(positionStep + 4),

                (StreamKind.Data, ColumnTypeKind.Decimal, _) => 0,
                (StreamKind.Data, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => GetRowEntryPosition(positionStep + 2),
                (StreamKind.Data, ColumnTypeKind.String, ColumnEncodingKind.DirectV2) => 0,
                (StreamKind.Data, ColumnTypeKind.Short, _) => GetRowEntryPosition(positionStep + 2),
                (StreamKind.Data, ColumnTypeKind.Long, _) => GetRowEntryPosition(positionStep + 2),
                (StreamKind.Data, ColumnTypeKind.Int, _) => GetRowEntryPosition(positionStep + 2),

                _ => GetRowEntryPosition(positionStep + 2)
                //_ => throw new NotImplementedException()
            };

            return new StreamPositions(rowGroupOffset, rowEntryOffset, valuesToSkip);
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

        private int GetRowEntryPosition(int positionIndex) => (int)_readerContext.RowIndexEntry.Positions[positionIndex];

        private BigInteger? ReadBigVarInt(ref SequenceReader<byte> stream)
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
