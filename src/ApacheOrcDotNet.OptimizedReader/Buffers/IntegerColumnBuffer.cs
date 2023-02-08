using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using System;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    public class IntegerColumnBuffer : BaseColumnBuffer<long?>
    {
        private readonly byte[] _presentRleBuffer = new byte[Constants.RleBufferMaxLength];
        private readonly long[] _dataRleBuffer = new long[Constants.RleBufferMaxLength];

        private readonly bool[] _presentStreamValues;
        private readonly long[] _dataStreamValues;

        private byte[] _dataStreamCompressedBuffer;
        private byte[] _dataStreamDecompressedBuffer;
        private int _dataStreamDecompressedBufferLength;

        private byte[] _presentStreamCompressedBuffer;
        private byte[] _presentStreamDecompressedBuffer;
        private int _presentStreamDecompressedBufferLength;

        public IntegerColumnBuffer(IByteRangeProvider byteRangeProvider, OrcFileProperties orcFileProperties, OrcColumn column) : base(byteRangeProvider, orcFileProperties, column)
        {
            _presentStreamValues = new bool[orcFileProperties.MaxValuesToRead];
            _dataStreamValues = new long[orcFileProperties.MaxValuesToRead];

            _dataStreamCompressedBuffer = new byte[orcFileProperties.ReusableBufferLength];
            _dataStreamDecompressedBuffer = new byte[orcFileProperties.ReusableBufferLength];

            _presentStreamCompressedBuffer = new byte[orcFileProperties.ReusableBufferLength];
            _presentStreamDecompressedBuffer = new byte[orcFileProperties.ReusableBufferLength];
        }

        public override async Task LoadDataAsync(int stripeId, ColumnDataStreams streams)
        {
            CheckByteRangeBufferLength(streams.Present, ref _presentStreamCompressedBuffer);
            CheckByteRangeBufferLength(streams.Data, ref _dataStreamCompressedBuffer);

            await Task.WhenAll(
                GetByteRangeAsync(streams.Present, _presentStreamCompressedBuffer),
                GetByteRangeAsync(streams.Data, _dataStreamCompressedBuffer)
            );

            DecompressByteRange(streams.Present, _presentStreamCompressedBuffer, ref _presentStreamDecompressedBuffer, ref _presentStreamDecompressedBufferLength);
            DecompressByteRange(streams.Data, _dataStreamCompressedBuffer, ref _dataStreamDecompressedBuffer, ref _dataStreamDecompressedBufferLength);

            Fill(streams);
        }

        private void Fill(ColumnDataStreams streams)
        {
            ReadBooleanStream(streams.Present, _presentStreamDecompressedBuffer.AsSpan()[.._presentStreamDecompressedBufferLength], _presentRleBuffer, _presentStreamValues, out var presentValuesRead);
            ReadNumericStream(streams.Data, _dataStreamDecompressedBuffer.AsSpan()[.._dataStreamDecompressedBufferLength], _dataRleBuffer, isSigned: true, _dataStreamValues, out var dataValuesRead);

            if (presentValuesRead > 0)
            {
                var dataIndex = 0;
                for (int idx = 0; idx < presentValuesRead; idx++)
                {
                    if (_presentStreamValues[idx])
                        _values[_numValuesRead++] = _dataStreamValues[dataIndex++];
                    else
                        _values[_numValuesRead++] = null;
                }
            }
            else
            {
                for (int idx = 0; idx < dataValuesRead; idx++)
                    _values[_numValuesRead++] = _dataStreamValues[idx];
            }
        }
    }
}
