using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System.Buffers;
using System.Collections.Generic;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    public class BinaryColumnBuffer : BaseColumnBuffer<byte[]>
    {
        private readonly bool[] _presentStreamBuffer;
        private readonly long[] _lengthStreamBuffer;

        public BinaryColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContext context, OrcColumn column) : base(byteRangeProvider, context, column)
        {
            _presentStreamBuffer = new bool[_context.MaxValuesToRead];
            _lengthStreamBuffer = new long[_context.MaxValuesToRead];
        }

        public override void Fill(int stripeId, IEnumerable<StreamDetail> columnStreams, RowIndexEntry rowIndexEntry)
        {
            ResetInnerBuffers();

            var presentStream = GetStripeStream(columnStreams, StreamKind.Present, isRequired: false);
            var lengthStream = GetStripeStream(columnStreams, StreamKind.Length);
            var dataStream = GetStripeStream(columnStreams, StreamKind.Data);

            // Positions
            var presentPositions = GetPresentStreamPositions(presentStream, rowIndexEntry);
            var lengthPositions = GetTargetedStreamPositions(presentStream, lengthStream, rowIndexEntry);
            var dataPositions = GetTargetedStreamPositions(presentStream, dataStream, rowIndexEntry);

            // Decompression
            var presentMemory = _byteRangeProvider.DecompressByteRangeNew(_context, presentStream, in presentPositions).Sequence;
            var lengthMemory = _byteRangeProvider.DecompressByteRangeNew(_context, lengthStream, in lengthPositions).Sequence;
            var dataMemory = _byteRangeProvider.DecompressByteRangeNew(_context, dataStream, in dataPositions).Sequence;

            // Processing
            var numPresentValuesRead = ReadBooleanStream(in presentMemory, presentPositions, _presentStreamBuffer);
            var numLengthValuesRead = ReadNumericStream(in lengthMemory, lengthPositions, isSigned: false, _lengthStreamBuffer);

            var rowEntryLength = dataMemory.Length - dataPositions.RowEntryOffset;
            var dataSequence = dataMemory.Slice(dataPositions.RowEntryOffset, rowEntryLength);

            var stringOffset = 0;
            if (presentStream != null)
            {
                var lengthIndex = 0;
                for (int idx = 0; idx < numPresentValuesRead; idx++)
                {
                    if (_presentStreamBuffer[idx])
                    {
                        var length = (int)_lengthStreamBuffer[lengthIndex++];
                        _values[_numValuesRead++] = dataSequence.Slice(stringOffset, length).ToArray();
                        stringOffset += length;
                    }
                    else
                        _values[_numValuesRead++] = null;

                    if (_numValuesRead >= _values.Length)
                        break;
                }
            }
            else
            {
                for (int idx = 0; idx < numLengthValuesRead; idx++)
                {
                    var length = (int)_lengthStreamBuffer[idx];
                    _values[_numValuesRead++] = dataSequence.Slice(stringOffset, length).ToArray();
                    stringOffset += length;

                    if (_numValuesRead >= _values.Length)
                        break;
                }
            }
        }
    }
}
