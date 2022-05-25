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

        public DoubleColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContextNew context, OrcColumn column) : base(byteRangeProvider, context, column)
        {
            _presentStreamBuffer = new bool[_context.MaxValuesToRead];
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
}