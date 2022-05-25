using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System.Collections.Generic;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    public class IntegerColumnBuffer : BaseColumnBuffer<long?>
    {
        private bool[] _presentStreamBuffer;
        private long[] _dataStreamBuffer;

        public IntegerColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContextNew context, OrcColumn column) : base(byteRangeProvider, context, column)
        {
            _presentStreamBuffer = new bool[_context.MaxValuesToRead];
            _dataStreamBuffer = new long[_context.MaxValuesToRead];
        }

        public override void Fill(int stripeId, IEnumerable<StreamDetail> columnStreams, RowIndexEntry rowIndexEntry)
        {
            ResetInnerBuffers();

            var presentStream = GetStripeStream(columnStreams, StreamKind.Present, isRequired: false);
            var dataStream = GetStripeStream(columnStreams, StreamKind.Data);

            // Present
            var presentPositions = GetPresentStreamPositions(presentStream, rowIndexEntry);
            var numPresentValuesRead = ReadBooleanStream(presentStream, presentPositions, _presentStreamBuffer);

            // Data
            var dataPostions = GetTargetedStreamPositions(presentStream, dataStream, rowIndexEntry);
            var numDataValuesRead = ReadNumericStream(dataStream, dataPostions, isSigned: true, _dataStreamBuffer);

            var dataIndex = 0;
            if (presentStream != null)
            {
                for (int idx = 0; idx < numPresentValuesRead; idx++)
                {
                    if (_presentStreamBuffer[idx])
                        _values[_numValuesRead++] = _dataStreamBuffer[dataIndex++];
                    else
                        _values[_numValuesRead++] = null;
                }
            }
            else
            {
                for (int idx = 0; idx < numDataValuesRead; idx++)
                    _values[_numValuesRead++] = _dataStreamBuffer[idx];
            }
        }
    }
}