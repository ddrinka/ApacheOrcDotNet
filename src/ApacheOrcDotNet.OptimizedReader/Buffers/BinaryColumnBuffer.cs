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

        public BinaryColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContextNew context, OrcColumn column) : base(byteRangeProvider, context, column)
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

            // Present
            var presentPositions = GetPresentStreamPositions(presentStream, rowIndexEntry);
            var numPresentValuesRead = ReadBooleanStream(presentStream, presentPositions, _presentStreamBuffer);

            // Length
            var lengthPositions = GetTargetedStreamPositions(presentStream, lengthStream, rowIndexEntry);
            var numLengthValuesRead = ReadNumericStream(lengthStream, lengthPositions, isSigned: false, _lengthStreamBuffer);

            // Data
            var dataStreamPostions = GetTargetedStreamPositions(presentStream, dataStream, rowIndexEntry);
            var dataBuffer = _byteRangeProvider.DecompressByteRange(
                offset: dataStream.FileOffset + dataStreamPostions.RowGroupOffset,
                compressedLength: dataStream.Length - dataStreamPostions.RowGroupOffset,
                compressionKind: _context.CompressionKind,
                compressionBlockSize: _context.CompressionBlockSize
            );

            using (dataBuffer)
            {
                var rowEntryLength = dataBuffer.Sequence.Length - dataStreamPostions.RowEntryOffset;
                var dataSequence = dataBuffer.Sequence.Slice(dataStreamPostions.RowEntryOffset, rowEntryLength);

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
}
