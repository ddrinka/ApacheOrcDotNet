﻿using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    public class IntegerColumnBuffer : BaseColumnBuffer<long?>
    {
        private readonly bool[] _presentStreamValues;
        private readonly long[] _dataStreamValues;

        private byte[] _dataStreamCompressedBuffer;
        private byte[] _dataStreamDecompressedBuffer;
        private int _dataStreamDecompressedBufferLength;

        private byte[] _presentStreamCompressedBuffer;
        private byte[] _presentStreamDecompressedBuffer;
        private int _presentStreamDecompressedBufferLength;

        private ColumnDataStreams _streams;

        public IntegerColumnBuffer(IByteRangeProvider byteRangeProvider, OrcFileProperties orcFileProperties, OrcColumn column) : base(byteRangeProvider, orcFileProperties, column)
        {
            _presentStreamValues = new bool[_orcFileProperties.MaxValuesToRead];
            _dataStreamValues = new long[_orcFileProperties.MaxValuesToRead];

            _dataStreamCompressedBuffer = _pool.Rent(_orcFileProperties.MaxCompressedBufferLength);
            _dataStreamDecompressedBuffer = _pool.Rent(_orcFileProperties.MaxDecompressedBufferLength);

            _presentStreamCompressedBuffer = _pool.Rent(_orcFileProperties.MaxCompressedBufferLength);
            _presentStreamDecompressedBuffer = _pool.Rent(_orcFileProperties.MaxDecompressedBufferLength);
        }

        public override async Task LoadDataAsync(int stripeId, ColumnDataStreams streams)
        {
            _streams = streams;

            _ = await Task.WhenAll(
                GetByteRangeAsync(_streams.Present, _presentStreamCompressedBuffer),
                GetByteRangeAsync(_streams.Data, _dataStreamCompressedBuffer)
            );

            DecompressByteRange(_streams.Present, _presentStreamCompressedBuffer, _presentStreamDecompressedBuffer, ref _presentStreamDecompressedBufferLength);
            DecompressByteRange(_streams.Data, _dataStreamCompressedBuffer, _dataStreamDecompressedBuffer, ref _dataStreamDecompressedBufferLength);
        }

        public override void Fill()
        {
            ReadBooleanStream(_streams.Present, _presentStreamDecompressedBuffer, _presentStreamDecompressedBufferLength, _presentStreamValues, out var presentValuesRead);
            ReadNumericStream(_streams.Data, _dataStreamDecompressedBuffer, _dataStreamDecompressedBufferLength, isSigned: true, _dataStreamValues, out var dataValuesRead);

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