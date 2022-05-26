using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    public class DateColumnBuffer : BaseColumnBuffer<DateTime?>
    {
        readonly static DateTime _unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private readonly bool[] _presentStreamBuffer;
        private readonly long[] _dataStreamBuffer;

        public DateColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContext context, OrcColumn column) : base(byteRangeProvider, context, column)
        {
            _presentStreamBuffer = new bool[_context.MaxValuesToRead];
            _dataStreamBuffer = new long[_context.MaxValuesToRead];
        }

        public override void Fill(int stripeId, IEnumerable<StreamDetail> columnStreams, RowIndexEntry rowIndexEntry)
        {
            ResetInnerBuffers();

            var presentStream = GetStripeStream(columnStreams, StreamKind.Present, isRequired: false);
            var dataStream = GetStripeStream(columnStreams, StreamKind.Data);

            // Positions
            var presentPositions = GetPresentStreamPositions(presentStream, rowIndexEntry);
            var dataPositions = GetTargetedStreamPositions(presentStream, dataStream, rowIndexEntry);

            // Decompression
            var presentMemory = _byteRangeProvider.DecompressByteRangeNew(_context, presentStream, in presentPositions).Sequence;
            var dataMemory = _byteRangeProvider.DecompressByteRangeNew(_context, dataStream, in dataPositions).Sequence;

            // Processing
            var numPresentValuesRead = ReadBooleanStream(in presentMemory, presentPositions, _presentStreamBuffer);
            var numDataValuesRead = ReadNumericStream(in dataMemory, dataPositions, isSigned: true, _dataStreamBuffer);

            if (presentStream != null)
            {
                var dataIndex = 0;
                for (int idx = 0; idx < numPresentValuesRead; idx++)
                {
                    if (_presentStreamBuffer[idx])
                        _values[_numValuesRead++] = _unixEpoch.AddTicks(_dataStreamBuffer[dataIndex++] * TimeSpan.TicksPerDay);
                    else
                        _values[_numValuesRead++] = null;
                }
            }
            else
            {
                for (int idx = 0; idx < numDataValuesRead; idx++)
                    _values[_numValuesRead++] = _unixEpoch.AddTicks(_dataStreamBuffer[idx] * TimeSpan.TicksPerDay);
            }
        }
    }
}