using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System;
using System.Buffers;
using System.Collections.Generic;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    public class DoubleColumnBuffer : BaseColumnBuffer<double>
    {
        private bool[] _presentStreamBuffer;

        public DoubleColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContext context, OrcColumn column) : base(byteRangeProvider, context, column)
        {
            _presentStreamBuffer = new bool[_context.MaxValuesToRead];
        }

        public override void Fill(int stripeId, IEnumerable<StreamDetail> columnStreams, RowIndexEntry rowIndexEntry)
        {
            ResetInnerBuffers();

            var presentStream = GetStripeStream(columnStreams, StreamKind.Present, isRequired: false);
            var dataStream = GetStripeStream(columnStreams, StreamKind.Data);

            // Positions
            var presentPositions = GetPresentStreamPositions(presentStream, rowIndexEntry);
            var dataStreamPostions = GetTargetedStreamPositions(presentStream, dataStream, rowIndexEntry);

            // Decompression
            var presentMemory = _byteRangeProvider.DecompressByteRangeNew(_context, presentStream, presentPositions).Sequence;
            var dataMemory = _byteRangeProvider.DecompressByteRangeNew(_context, dataStream, dataStreamPostions).Sequence;

            // Processing
            var numPresentValuesRead = ReadBooleanStream(in presentMemory, presentPositions, _presentStreamBuffer);

            var rowEntryLength = dataMemory.Length - dataStreamPostions.RowEntryOffset;
            var dataSequence = dataMemory.Slice(dataStreamPostions.RowEntryOffset, rowEntryLength);
            var dataReader = new SequenceReader<byte>(dataSequence);

            Span<byte> valueBuffer = stackalloc byte[8];
            if (presentStream != null)
            {
                for (int idx = 0; idx < numPresentValuesRead; idx++)
                {
                    if (_presentStreamBuffer[idx])
                    {
                        dataReader.TryCopyTo(valueBuffer);
                        _values[_numValuesRead++] = BitConverter.ToDouble(valueBuffer);
                        dataReader.Advance(valueBuffer.Length);
                    }
                    else
                        _values[_numValuesRead++] = double.NaN;
                }
            }
            else
            {
                while (dataReader.TryCopyTo(valueBuffer))
                {
                    dataReader.Advance(valueBuffer.Length);

                    _values[_numValuesRead++] = BitConverter.ToDouble(valueBuffer);

                    if (_numValuesRead >= _values.Length)
                        break;
                }
            }
        }
    }
}