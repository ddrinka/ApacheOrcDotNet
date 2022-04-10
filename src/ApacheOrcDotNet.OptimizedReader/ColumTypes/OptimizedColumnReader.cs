using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.OptimizedReader.ColumTypes.Specialized;
using ApacheOrcDotNet.OptimizedReader.Encodings;
using ApacheOrcDotNet.Protocol;
using System;
using System.Buffers;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace ApacheOrcDotNet.OptimizedReader.ColumTypes
{
    public abstract class OptimizedColumnReader
    {
        private readonly SpanFileTail _fileTail;
        private readonly IByteRangeProvider _byteRangeProvider;
        private readonly ReaderContextOld _readContext;

        public OptimizedColumnReader(SpanFileTail fileTail, IByteRangeProvider byteRangeProvider, ReaderContextOld readContext)
        {
            _fileTail = fileTail;
            _byteRangeProvider = byteRangeProvider;
            _readContext = readContext;
        }

        protected ColumnEncodingKind? GetColumnEncodingKind(StreamKind streamKind)
        {
            var stripeStream = GetStripeStream(streamKind);
            if (stripeStream == null)
                return null;
            return stripeStream.EncodingKind;
        }

        protected StreamDetail GetStripeStream(StreamKind streamKind, bool isRequired = true)
        {
            var stream = _readContext.Streams.SingleOrDefault(stream =>
                stream.ColumnId == _readContext.Column.ColumnId
                && stream.StreamKind == streamKind
            );

            if (isRequired && stream == null)
                throw new InvalidDataException($"The '{streamKind}' stream must be available");

            return stream;
        }

        [SkipLocalsInit]
        protected int ReadNumericStream(StreamDetail stream, in StreamPositions positions, bool isSigned, Span<long> outputValues)
        {
            var numValuesRead = 0;

            if (stream == null)
                return numValuesRead;

            if (stream.EncodingKind != ColumnEncodingKind.DirectV2 && stream.EncodingKind != ColumnEncodingKind.DictionaryV2)
                throw new NotImplementedException($"Unimplemented Numeric {nameof(ColumnEncodingKind)} {stream.EncodingKind}");

            int skippedValues = 0;

            var dataBuffer = _byteRangeProvider.DecompressByteRange(
                offset: stream.FileOffset + positions.RowGroupOffset,
                compressedLength: stream.Length - positions.RowGroupOffset,
                compressionKind: _fileTail.PostScript.Compression,
                compressionBlockSize: (int)_fileTail.PostScript.CompressionBlockSize
            );

            var valuesBuffer = ArrayPool<long>.Shared.Rent(1_000);
            var valuesBufferSpan = valuesBuffer.AsSpan().Slice(0, 1_000);
            var rowEntryLength = dataBuffer.Sequence.Length - positions.RowEntryOffset;
            var dataSequence = dataBuffer.Sequence.Slice(positions.RowEntryOffset, rowEntryLength);
            var dataReader = new SequenceReader<byte>(dataSequence);

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

        [SkipLocalsInit]
        protected int ReadBooleanStream(StreamDetail stream, in StreamPositions positions, Span<bool> outputValues)
        {
            if (stream == null)
                return 0;

            var dataBuffer = _byteRangeProvider.DecompressByteRange(
                offset: stream.FileOffset + positions.RowGroupOffset,
                compressedLength: stream.Length - positions.RowGroupOffset,
                compressionKind: _fileTail.PostScript.Compression,
                compressionBlockSize: (int)_fileTail.PostScript.CompressionBlockSize
            );

            var valuesBuffer = ArrayPool<byte>.Shared.Rent(1_000);
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

                    var decodedByte = valuesBufferSpan[idx];

                    outputValues[numValuesRead++] = (decodedByte & 0x80) != 0;
                    if (numValuesRead >= outputValues.Length)
                        return numValuesRead;

                    outputValues[numValuesRead++] = (decodedByte & 0x40) != 0;
                    if (numValuesRead >= outputValues.Length)
                        return numValuesRead;

                    outputValues[numValuesRead++] = (decodedByte & 0x20) != 0;
                    if (numValuesRead >= outputValues.Length)
                        return numValuesRead;

                    outputValues[numValuesRead++] = (decodedByte & 0x10) != 0;
                    if (numValuesRead >= outputValues.Length)
                        return numValuesRead;

                    outputValues[numValuesRead++] = (decodedByte & 0x08) != 0;
                    if (numValuesRead >= outputValues.Length)
                        return numValuesRead;

                    outputValues[numValuesRead++] = (decodedByte & 0x04) != 0;
                    if (numValuesRead >= outputValues.Length)
                        return numValuesRead;

                    outputValues[numValuesRead++] = (decodedByte & 0x02) != 0;
                    if (numValuesRead >= outputValues.Length)
                        return numValuesRead;

                    outputValues[numValuesRead++] = (decodedByte & 0x01) != 0;
                    if (numValuesRead >= outputValues.Length)
                        return numValuesRead;
                }
            }

            return numValuesRead;
        }

        [SkipLocalsInit]
        protected void ReadByteStream(StreamKind streamKind, Span<byte> outputValues, out int numValuesRead)
        {
            numValuesRead = 0;

            var stream = GetStripeStream(streamKind);
            if (stream == null)
                return;

            int skippedValues = 0;
            int rowGroupOffset = streamKind switch
            {
                StreamKind.Secondary => _readContext.GetRowEntryPosition(2),
                StreamKind.Length => _readContext.GetRowEntryPosition(0),
                StreamKind.Data => _readContext.GetRowEntryPosition(0),
                _ => throw new NotImplementedException()
            };
            int rowEntryOffset = (streamKind, _readContext.Column.ColumnType) switch
            {
                (StreamKind.Length, ColumnTypeKind.String) => 0,
                (StreamKind.Secondary, _) => _readContext.GetRowEntryPosition(3),
                (StreamKind.Data, _) => _readContext.GetRowEntryPosition(1),
                _ => throw new NotImplementedException()
            };
            int valuesToSkip = (streamKind, _readContext.Column.ColumnType) switch
            {
                (StreamKind.Length, ColumnTypeKind.String) => 0,
                (StreamKind.Secondary, _) => _readContext.GetRowEntryPosition(4),
                (StreamKind.Data, _) => _readContext.GetRowEntryPosition(2),
                _ => throw new NotImplementedException()
            };

            var dataBuffer = _byteRangeProvider.DecompressByteRange(
                offset: stream.FileOffset + rowGroupOffset,
                compressedLength: stream.Length - rowGroupOffset,
                compressionKind: _fileTail.PostScript.Compression,
                compressionBlockSize: (int)_fileTail.PostScript.CompressionBlockSize
            );

            var valuesBuffer = ArrayPool<byte>.Shared.Rent(1_000);
            var valuesBufferSpan = valuesBuffer.AsSpan().Slice(0, 1_000);
            var dataReader = new SequenceReader<byte>(dataBuffer.Sequence);

            dataReader.Advance(rowEntryOffset);

            while (!dataReader.End)
            {
                var numByteValuesRead = OptimizedByteRunLengthEncodingReader.ReadValues(
                    ref dataReader,
                    valuesBufferSpan
                );

                for (int idx = 0; idx < numByteValuesRead; idx++)
                {
                    if (skippedValues++ < valuesToSkip)
                        continue;

                    outputValues[numValuesRead++] = valuesBufferSpan[idx];

                    if (numValuesRead >= outputValues.Length)
                        return;
                }
            }
        }

        [SkipLocalsInit]
        protected int ReadVarIntStream(StreamDetail stream, StreamPositions positions, Span<BigInteger> outputValues)
        {
            var numValuesRead = 0;

            if (stream == null)
                return numValuesRead;

            int skippedValues = 0;

            var dataBuffer = _byteRangeProvider.DecompressByteRange(
                offset: stream.FileOffset + positions.RowGroupOffset,
                compressedLength: stream.Length - positions.RowGroupOffset,
                compressionKind: _fileTail.PostScript.Compression,
                compressionBlockSize: (int)_fileTail.PostScript.CompressionBlockSize
            );

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

        private static BigInteger? ReadBigVarInt(ref SequenceReader<byte> stream)
        {
            var value = BigInteger.Zero;
            long currentLong = 0;
            int bitCount = 0;

            while (true)
            {
                if (!stream.TryRead(out var currentByte))
                    return null; // Reached the end of the stream

                currentLong |= ((long)currentByte & 0x7f) << (bitCount % 63);
                bitCount += 7;

                if (bitCount % 63 == 0)
                {
                    if (bitCount == 63)
                        value = new BigInteger(currentLong);
                    else
                        value |= new BigInteger(currentLong) << (bitCount - 63);

                    currentLong = 0;
                }

                // Done when the high bit is set
                if (currentByte < 0x80)
                    break;
            }

            if (currentLong != 0) // Some bits left to add to result
            {
                var shift = (bitCount / 63) * 63;
                value |= new BigInteger(currentLong) << shift;
            }

            // Un zig-zag
            return ((long)value).ZigzagDecode();
        }
    }
}
