using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    [SkipLocalsInit]
    public class TimestampColumnBuffer : BaseColumnBuffer<DateTime?>
    {
        readonly static DateTime _orcEpoch = new DateTime(2015, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private readonly bool[] _presentStreamBuffer;
        private readonly long[] _dataStreamBuffer;
        private long[] _secondaryStreamBuffer;
        private byte[] _presentInputBuffer;
        private byte[] _presentOutputBuffer;
        private byte[] _dataInputBuffer;
        private byte[] _dataOutputBuffer;
        private byte[] _secondaryInputBuffer;
        private byte[] _secondaryOutputBuffer;

        public TimestampColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContext context, OrcColumn column) : base(byteRangeProvider, context, column)
        {
            _presentStreamBuffer = new bool[_context.MaxValuesToRead];
            _dataStreamBuffer = new long[_context.MaxValuesToRead];
            _secondaryStreamBuffer = new long[_context.MaxValuesToRead];

            _presentInputBuffer = _arrayPool.Rent(_context.MaxCompressedBufferLength);
            _presentOutputBuffer = _arrayPool.Rent(_context.MaxDecompresseBufferLength);

            _dataInputBuffer = _arrayPool.Rent(_context.MaxCompressedBufferLength);
            _dataOutputBuffer = _arrayPool.Rent(_context.MaxDecompresseBufferLength);

            _secondaryInputBuffer = _arrayPool.Rent(_context.MaxCompressedBufferLength);
            _secondaryOutputBuffer = _arrayPool.Rent(_context.MaxDecompresseBufferLength);
        }

        public override void Fill(int stripeId, IEnumerable<StreamDetail> columnStreams, RowIndexEntry rowIndexEntry)
        {
            // Streams
            var presentStream = GetColumnStream(columnStreams, StreamKind.Present, isRequired: false);
            var dataStream = GetColumnStream(columnStreams, StreamKind.Data);
            var secondaryStream = GetColumnStream(columnStreams, StreamKind.Secondary);

            // Stream Positions
            var presentPositions = GetPresentStreamPositions(presentStream, rowIndexEntry);
            var dataPositions = GetTargetDataStreamPositions(presentStream, dataStream, rowIndexEntry);
            var secondaryPostions = GetTargetDataStreamPositions(presentStream, secondaryStream, rowIndexEntry);

            // Stream Byte Ranges
            (int present, int data, int secondary) rangeSizes = default;
            Parallel.Invoke(
                () => GetByteRange(_presentInputBuffer, presentStream, presentPositions, ref rangeSizes.present),
                () => GetByteRange(_dataInputBuffer, dataStream, dataPositions, ref rangeSizes.data),
                () => GetByteRange(_secondaryInputBuffer, secondaryStream, secondaryPostions, ref rangeSizes.secondary)
            );

            // Decompress Byte Ranges
            (int present, int data, int secondary) decompressedSizes = default;
            DecompressByteRange(_presentInputBuffer, _presentOutputBuffer, presentStream, presentPositions, ref decompressedSizes.present);
            DecompressByteRange(_dataInputBuffer, _dataOutputBuffer, dataStream, dataPositions, ref decompressedSizes.data);
            DecompressByteRange(_secondaryInputBuffer, _secondaryOutputBuffer, secondaryStream, secondaryPostions, ref decompressedSizes.secondary);

            // Parse Decompressed Bytes
            (int present, int data, int secondary) valuesRead = default;
            ReadBooleanStream(_presentOutputBuffer, decompressedSizes.present, presentPositions, _presentStreamBuffer, ref valuesRead.present);
            ReadNumericStream(_dataOutputBuffer, decompressedSizes.data, dataPositions, isSigned: true, _dataStreamBuffer, ref valuesRead.data);
            ReadNumericStream(_secondaryOutputBuffer, decompressedSizes.secondary, secondaryPostions, isSigned: false, _secondaryStreamBuffer, ref valuesRead.secondary);

            if (presentStream != null)
            {
                var secondaryIndex = 0;
                for (int idx = 0; idx < valuesRead.present; idx++)
                {
                    if (_presentStreamBuffer[idx])
                    {
                        var seconds = _dataStreamBuffer[secondaryIndex];
                        var nanosecondTicks = EncodedNanosToTicks(_secondaryStreamBuffer[secondaryIndex]);
                        var totalTicks = seconds * TimeSpan.TicksPerSecond + (seconds >= 0 ? nanosecondTicks : -nanosecondTicks);
                        _values[_numValuesRead++] = _orcEpoch.AddTicks(totalTicks);
                        secondaryIndex++;
                    }
                    else
                        _values[_numValuesRead++] = null;
                }
            }
            else
            {
                for (int idx = 0; idx < valuesRead.data; idx++)
                {
                    var seconds = _dataStreamBuffer[idx];
                    var nanosecondTicks = EncodedNanosToTicks(_secondaryStreamBuffer[idx]);
                    var totalTicks = seconds * TimeSpan.TicksPerSecond + (seconds >= 0 ? nanosecondTicks : -nanosecondTicks);
                    _values[_numValuesRead++] = _orcEpoch.AddTicks(totalTicks);
                }
            }
        }

        private long EncodedNanosToTicks(long encodedNanos)
        {
            var scale = (int)(encodedNanos & 0x7);
            var nanos = encodedNanos >> 3;

            if (scale == 0)
                return nanos;

            while (scale-- >= 0)
                nanos *= 10;

            return nanos / 100;     //100 nanoseconds per tick
        }
    }
}
