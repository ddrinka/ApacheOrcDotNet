using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    public class TimestampColumnBuffer : BaseColumnBuffer<DateTime?>
    {
        readonly static DateTime _orcEpoch = new DateTime(2015, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private readonly bool[] _presentStreamBuffer;
        private readonly long[] _dataStreamBuffer;
        private long[] _secondaryStreamBuffer;

        public TimestampColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContext context, OrcColumn column) : base(byteRangeProvider, context, column)
        {
            _presentStreamBuffer = new bool[_context.MaxValuesToRead];
            _dataStreamBuffer = new long[_context.MaxValuesToRead];
            _secondaryStreamBuffer = new long[_context.MaxValuesToRead];
        }

        public override void Fill(int stripeId, IEnumerable<StreamDetail> columnStreams, RowIndexEntry rowIndexEntry)
        {
            ResetInnerBuffers();

            var presentStream = GetStripeStream(columnStreams, StreamKind.Present, isRequired: false);
            var dataStream = GetStripeStream(columnStreams, StreamKind.Data);
            var secondaryStream = GetStripeStream(columnStreams, StreamKind.Secondary);

            // Positions
            var presentPositions = GetPresentStreamPositions(presentStream, rowIndexEntry);
            var dataPositions = GetTargetedStreamPositions(presentStream, dataStream, rowIndexEntry);
            var secondaryPostions = GetTargetedStreamPositions(presentStream, secondaryStream, rowIndexEntry);

            // Decompression
            var presentMemory = _byteRangeProvider.DecompressByteRangeNew(_context, presentStream, in presentPositions).Sequence;
            var dataMemory = _byteRangeProvider.DecompressByteRangeNew(_context, dataStream, in dataPositions).Sequence;
            var secondaryMemory = _byteRangeProvider.DecompressByteRangeNew(_context, secondaryStream, in secondaryPostions).Sequence;

            // Processing
            var numPresentValuesRead = ReadBooleanStream(in presentMemory, presentPositions, _presentStreamBuffer);
            var numDataValuesRead = ReadNumericStream(in dataMemory, dataPositions, isSigned: true, _dataStreamBuffer);
            _ = ReadNumericStream(in secondaryMemory, secondaryPostions, isSigned: false, _secondaryStreamBuffer);

            if (presentStream != null)
            {
                var secondaryIndex = 0;
                for (int idx = 0; idx < numPresentValuesRead; idx++)
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
                for (int idx = 0; idx < numDataValuesRead; idx++)
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
