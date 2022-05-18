using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.Protocol;
using System;
using System.Buffers;

namespace ApacheOrcDotNet.OptimizedReader.ColumTypes
{
    public class OptimizedDoubleReader : BaseColumnReader<double>
    {
        public OptimizedDoubleReader(ReaderContext readerContext) : base(readerContext)
        {
        }

        public override void FillBuffer()
        {
            var presentStream = GetStripeStream(StreamKind.Present, isRequired: false);
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
                if (presentStream != null)
                {
                    for (int idx = 0; idx < numPresentValuesRead; idx++)
                    {
                        if (presentBuffer[idx])
                        {
                            _outputValuesRaw[_numValuesRead++] = dataBuffer.ReadDouble(dataIndex);
                            dataIndex += 8;
                        }
                        else
                            _outputValuesRaw[_numValuesRead++] = double.NaN;
                    }
                }
                else
                {
                    for (int idx = 0; idx < numDataValuesRead; idx++)
                    {
                        _outputValuesRaw[_numValuesRead++] = dataBuffer.ReadDouble(dataIndex);
                        dataIndex += 8;
                    }
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