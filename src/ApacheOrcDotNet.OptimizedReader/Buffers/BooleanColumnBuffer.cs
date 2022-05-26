﻿using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System.Collections.Generic;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    public class BooleanColumnBuffer : BaseColumnBuffer<bool?>
    {
        private bool[] _presentStreamBuffer;
        private bool[] _dataStreamBuffer;

        public BooleanColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContext context, OrcColumn column) : base(byteRangeProvider, context, column)
        {
            _presentStreamBuffer = new bool[_context.MaxValuesToRead];
            _dataStreamBuffer = new bool[_context.MaxValuesToRead];
        }

        public override void Fill(int stripeId, IEnumerable<StreamDetail> columnStreams, RowIndexEntry rowIndexEntry)
        {
            ResetInnerBuffers();

            var presentStream = GetStripeStream(columnStreams, StreamKind.Present, isRequired: false);
            var dataStream = GetStripeStream(columnStreams, StreamKind.Data);

            // Present
            var presentPositions = GetPresentStreamPositions(presentStream, rowIndexEntry);
            var dataPositions = GetTargetedStreamPositions(presentStream, dataStream, rowIndexEntry);

            // Decompression
            var presentMemory = _byteRangeProvider.DecompressByteRangeNew(_context, presentStream, in presentPositions).Sequence;
            var dataMemory = _byteRangeProvider.DecompressByteRangeNew(_context, dataStream, in dataPositions).Sequence;

            // Processing
            var numPresentValuesRead = ReadBooleanStream(in presentMemory, presentPositions, _presentStreamBuffer);
            var numDataValuesRead = ReadBooleanStream(in dataMemory, dataPositions, _dataStreamBuffer);

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