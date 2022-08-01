using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.OptimizedReader.Encodings;
using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using System;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    public abstract class BaseColumnBuffer<TOutput>
    {
        private readonly long[] _numericStreamBuffer;
        private readonly byte[] _byteStreamBuffer;
        private readonly byte[] _boolStreamBuffer;

        private protected readonly IByteRangeProvider _byteRangeProvider;
        private protected readonly OrcFileProperties _orcFileProperties;
        private protected readonly OrcColumn _column;
        private protected readonly TOutput[] _values;

        private protected int _numValuesRead;

        private StreamRange _lastRange;
        private int _lastRangeLength;

        public BaseColumnBuffer(IByteRangeProvider byteRangeProvider, OrcFileProperties orcFileProperties, OrcColumn column)
        {
            _byteRangeProvider = byteRangeProvider;
            _orcFileProperties = orcFileProperties;
            _column = column;
            _values = new TOutput[_orcFileProperties.MaxValuesToRead];

            // RLEs decode values
            // from at most two bytes.
            var runMaxValues = (int)Math.Pow(2, 16);
            _numericStreamBuffer = new long[runMaxValues];
            _byteStreamBuffer = new byte[runMaxValues];
            _boolStreamBuffer = new byte[runMaxValues];
        }

        public OrcColumn Column => _column;
        public ReadOnlySpan<TOutput> Values => _values.AsSpan()[.._numValuesRead];

        public abstract Task LoadDataAsync(int stripeId, ColumnDataStreams streams);

        public void Reset() => _numValuesRead = 0;

        private protected void ReadByteStream(StreamDetail stream, ReadOnlySpan<byte> decompressedBuffer, Span<byte> outputValues, out int numValuesRead)
        {
            numValuesRead = 0;

            if (stream == null)
                return;

            var numSkipped = 0;
            var bufferReader = new BufferReader(GetDataStream(stream, decompressedBuffer));

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

        private protected void ReadBooleanStream(StreamDetail stream, ReadOnlySpan<byte> decompressedBuffer, Span<bool> outputValues, out int numValuesRead)
        {
            numValuesRead = 0;

            if (stream == null)
                return;

            var numSkippedBits = 0;
            var numSkippedBytes = 0;
            var checkRemainingBits = true;
            var numOfBytesToSkip = stream.Positions.ValuesToSkip;
            var bufferReader = new BufferReader(GetDataStream(stream, decompressedBuffer));

            while (!bufferReader.Complete)
            {
                var numByteValuesRead = OptimizedByteRLE.ReadValues(ref bufferReader, _boolStreamBuffer);

                for (int idx = 0; idx < numByteValuesRead; idx++)
                {
                    if (numSkippedBytes++ < numOfBytesToSkip)
                        continue;

                    var decodedByte = _boolStreamBuffer[idx];

                    outputValues[numValuesRead++] = (decodedByte & 128) != 0;
                    if (checkRemainingBits && ++numSkippedBits <= stream.Positions.RemainingBits)
                        numValuesRead--;
                    if (numValuesRead >= outputValues.Length)
                        return;

                    outputValues[numValuesRead++] = (decodedByte & 64) != 0;
                    if (checkRemainingBits && ++numSkippedBits <= stream.Positions.RemainingBits)
                        numValuesRead--;
                    if (numValuesRead >= outputValues.Length)
                        return;

                    outputValues[numValuesRead++] = (decodedByte & 32) != 0;
                    if (checkRemainingBits && ++numSkippedBits <= stream.Positions.RemainingBits)
                        numValuesRead--;
                    if (numValuesRead >= outputValues.Length)
                        return;

                    outputValues[numValuesRead++] = (decodedByte & 16) != 0;
                    if (checkRemainingBits && ++numSkippedBits <= stream.Positions.RemainingBits)
                        numValuesRead--;
                    if (numValuesRead >= outputValues.Length)
                        return;

                    outputValues[numValuesRead++] = (decodedByte & 8) != 0;
                    if (checkRemainingBits && ++numSkippedBits <= stream.Positions.RemainingBits)
                        numValuesRead--;
                    if (numValuesRead >= outputValues.Length)
                        return;

                    outputValues[numValuesRead++] = (decodedByte & 4) != 0;
                    if (checkRemainingBits && ++numSkippedBits <= stream.Positions.RemainingBits)
                        numValuesRead--;
                    if (numValuesRead >= outputValues.Length)
                        return;

                    outputValues[numValuesRead++] = (decodedByte & 2) != 0;
                    if (checkRemainingBits && ++numSkippedBits <= stream.Positions.RemainingBits)
                        numValuesRead--;
                    if (numValuesRead >= outputValues.Length)
                        return;

                    outputValues[numValuesRead++] = (decodedByte & 1) != 0;
                    if (checkRemainingBits && ++numSkippedBits <= stream.Positions.RemainingBits)
                        return;
                    if (numValuesRead >= outputValues.Length)
                        return;

                    checkRemainingBits = false;
                }
            }
        }

        private protected void ReadNumericStream(StreamDetail stream, ReadOnlySpan<byte> decompressedBuffer, bool isSigned, Span<long> outputValues, out int numValuesRead)
        {
            numValuesRead = 0;

            if (stream == null)
                return;

            var numSkipped = 0;
            var bufferReader = new BufferReader(GetDataStream(stream, decompressedBuffer));

            while (!bufferReader.Complete)
            {
                var numNewValuesRead = OptimizedIntegerRLE.ReadValues(ref bufferReader, isSigned, _numericStreamBuffer);

                for (int idx = 0; idx < numNewValuesRead; idx++)
                {
                    if (numSkipped++ < stream.Positions.ValuesToSkip)
                        continue;

                    outputValues[numValuesRead++] = _numericStreamBuffer[idx];

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

        private protected async Task<int> GetByteRangeAsync(StreamDetail stream, Memory<byte> outputBuffer)
        {
            if (stream == null)
                return 0;

            // If current and last ranges are equal, the previous data will be buffered
            // and we can return only the length, without requesting the bytes again.

            if (stream.Range == _lastRange)
                return _lastRangeLength;

            await _byteRangeProvider.FillBufferAsync(outputBuffer[..stream.Range.Length], stream.Range.Offset);

            _lastRangeLength = stream.Range.Length;
            _lastRange = stream.Range;

            return _lastRangeLength;
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
