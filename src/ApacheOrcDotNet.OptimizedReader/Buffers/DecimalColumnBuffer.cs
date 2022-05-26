using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.Numerics;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    public class DecimalColumnBuffer : BaseColumnBuffer<decimal?>
    {
        private bool[] _presentStreamBuffer;
        private BigInteger[] _dataStreamBuffer;
        private long[] _secondaryStreamBuffer;

        public DecimalColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContext context, OrcColumn column) : base(byteRangeProvider, context, column)
        {
            _presentStreamBuffer = new bool[_context.MaxValuesToRead];
            _dataStreamBuffer = new BigInteger[_context.MaxValuesToRead];
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
            var dataPostions = GetTargetedStreamPositions(presentStream, dataStream, rowIndexEntry);
            var secondaryPostions = GetTargetedStreamPositions(presentStream, secondaryStream, rowIndexEntry);

            // Decompression
            var presentMemory = _byteRangeProvider.DecompressByteRangeNew(_context, presentStream, presentPositions).Sequence;
            var dataMemory = _byteRangeProvider.DecompressByteRangeNew(_context, dataStream, dataPostions).Sequence;
            var secondaryMemory = _byteRangeProvider.DecompressByteRangeNew(_context, secondaryStream, secondaryPostions).Sequence;

            // Processing
            var numPresentValuesRead = ReadBooleanStream(in presentMemory, presentPositions, _presentStreamBuffer);
            var numDataValuesRead = ReadVarIntStream(in dataMemory, dataPostions, _dataStreamBuffer);
            var numSecondaryValuesRead = ReadNumericStream(in secondaryMemory, secondaryPostions, isSigned: true, _secondaryStreamBuffer);

            var secondaryIndex = 0;
            if (presentStream != null)
            {
                for (int idx = 0; idx < numPresentValuesRead; idx++)
                {
                    if (_presentStreamBuffer[idx])
                    {
                        _values[_numValuesRead++] = BigIntegerToDecimal(_dataStreamBuffer[secondaryIndex], _secondaryStreamBuffer[secondaryIndex]);
                        secondaryIndex++;
                    }
                    else
                        _values[_numValuesRead++] = null;
                }
            }
            else
            {
                if (numDataValuesRead != numSecondaryValuesRead)
                    throw new InvalidOperationException("Number of values read from DATA and SECODARY streams do not match.");

                for (int idx = 0; idx < numSecondaryValuesRead; idx++)
                    _values[_numValuesRead++] = BigIntegerToDecimal(_dataStreamBuffer[idx], _secondaryStreamBuffer[idx]);
            }
        }
    }
}
