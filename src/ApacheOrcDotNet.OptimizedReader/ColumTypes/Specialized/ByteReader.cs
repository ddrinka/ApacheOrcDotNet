using ApacheOrcDotNet.Protocol;
using System;
using System.Buffers;

namespace ApacheOrcDotNet.OptimizedReader.ColumTypes.Specialized
{
    public class ByteReader : BaseColumnReader<byte?>
    {
        public ByteReader(ReaderContext readerContext) : base(readerContext)
        {
        }

        public override void FillBuffer()
        {
            var presentStreamRequired = _readerContext.RowIndexEntry.Statistics.HasNull;

            var presentStream = GetStripeStream(StreamKind.Present, presentStreamRequired);
            var dataStream = GetStripeStream(StreamKind.Data);

            var presentBuffer = ArrayPool<bool>.Shared.Rent(_numMaxValuesToRead);
            var dataBuffer = ArrayPool<byte>.Shared.Rent(_numMaxValuesToRead);

            try
            {
                // Present
                var presentPositions = GetPresentStreamPositions(presentStream);
                var numPresentValuesRead = ReadBooleanStream(presentStream, presentPositions, presentBuffer.AsSpan().Slice(0, _numMaxValuesToRead));

                // Data
                var dataPostions = GetTargetedStreamPositions(presentStream, dataStream);
                var numDataValuesRead = ReadByteStream(dataStream, dataPostions, dataBuffer.AsSpan().Slice(0, _numMaxValuesToRead));

                var dataIndex = 0;
                if (presentStreamRequired)
                {
                    for (int idx = 0; idx < numPresentValuesRead; idx++)
                    {
                        if (presentBuffer[idx])
                            _outputValuesRaw[_numValuesRead++] = dataBuffer[dataIndex++];
                        else
                            _outputValuesRaw[_numValuesRead++] = null;
                    }
                }
                else
                {
                    for (int idx = 0; idx < numDataValuesRead; idx++)
                        _outputValuesRaw[_numValuesRead++] = dataBuffer[idx];
                }
            }
            finally
            {
                ArrayPool<bool>.Shared.Return(presentBuffer);
                ArrayPool<byte>.Shared.Return(dataBuffer);
            }
        }
    }
}