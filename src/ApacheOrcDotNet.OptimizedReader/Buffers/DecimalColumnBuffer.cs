﻿using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using System;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    public class DecimalColumnBuffer : BaseColumnBuffer<decimal?>
    {
        private bool[] _presentStreamValues;
        private long[] _dataStreamValues;
        private long[] _secondaryStreamValues;

        private byte[] _dataStreamCompressedBuffer;
        private byte[] _dataStreamDecompressedBuffer;
        private int _dataStreamDecompressedBufferLength;

        private byte[] _presentStreamCompressedBuffer;
        private byte[] _presentStreamDecompressedBuffer;
        private int _presentStreamDecompressedBufferLength;

        private byte[] _secondaryStreamCompressedBuffer;
        private byte[] _secondaryStreamDecompressedBuffer;
        private int _secondaryStreamDecompressedBufferLength;

        private ColumnDataStreams _streams;

        public DecimalColumnBuffer(IByteRangeProvider byteRangeProvider, OrcFileProperties orcFileProperties, OrcColumn column) : base(byteRangeProvider, orcFileProperties, column)
        {
            _presentStreamValues = new bool[_orcFileProperties.MaxValuesToRead];
            _dataStreamValues = new long[_orcFileProperties.MaxValuesToRead];
            _secondaryStreamValues = new long[_orcFileProperties.MaxValuesToRead];

            _dataStreamCompressedBuffer = new byte[_orcFileProperties.MaxCompressedBufferLength];
            _dataStreamDecompressedBuffer = new byte[_orcFileProperties.MaxDecompressedBufferLength];

            _presentStreamCompressedBuffer = new byte[_orcFileProperties.MaxCompressedBufferLength];
            _presentStreamDecompressedBuffer = new byte[_orcFileProperties.MaxDecompressedBufferLength];

            _secondaryStreamCompressedBuffer = new byte[_orcFileProperties.MaxCompressedBufferLength];
            _secondaryStreamDecompressedBuffer = new byte[_orcFileProperties.MaxDecompressedBufferLength];
        }

        public override async Task LoadDataAsync(int stripeId, ColumnDataStreams streams)
        {
            _streams = streams;

            _ = await Task.WhenAll(
                GetByteRangeAsync(_streams.Present, _presentStreamCompressedBuffer),
                GetByteRangeAsync(_streams.Data, _dataStreamCompressedBuffer),
                GetByteRangeAsync(_streams.Secondary, _secondaryStreamCompressedBuffer)
            );

            DecompressByteRange(_streams.Present, _presentStreamCompressedBuffer, _presentStreamDecompressedBuffer, ref _presentStreamDecompressedBufferLength);
            DecompressByteRange(_streams.Data, _dataStreamCompressedBuffer, _dataStreamDecompressedBuffer, ref _dataStreamDecompressedBufferLength);
            DecompressByteRange(_streams.Secondary, _secondaryStreamCompressedBuffer, _secondaryStreamDecompressedBuffer, ref _secondaryStreamDecompressedBufferLength);
        }

        public override void Fill()
        {
            ReadBooleanStream(_streams.Present, _presentStreamDecompressedBuffer, _presentStreamDecompressedBufferLength, _presentStreamValues, out var presentValuesRead);
            ReadVarIntStream(_streams.Data, _dataStreamDecompressedBuffer, _dataStreamDecompressedBufferLength, _dataStreamValues, out var dataValuesRead);
            ReadNumericStream(_streams.Secondary, _secondaryStreamDecompressedBuffer, _secondaryStreamDecompressedBufferLength, isSigned: true, _secondaryStreamValues, out var secondaryValuesRead);

            if (dataValuesRead != secondaryValuesRead)
                throw new InvalidOperationException($"Number of data({dataValuesRead}) and secondary({secondaryValuesRead}) values must match.");

            if (presentValuesRead > 0)
            {
                var valueIndex = 0;
                for (int idx = 0; idx < presentValuesRead; idx++)
                {
                    if (_presentStreamValues[idx])
                    {
                        _values[_numValuesRead++] = VarIntToDecimal(_dataStreamValues[valueIndex], _secondaryStreamValues[valueIndex]);
                        valueIndex++;
                    }
                    else
                        _values[_numValuesRead++] = null;
                }
            }
            else
            {
                for (int idx = 0; idx < secondaryValuesRead; idx++)
                    _values[_numValuesRead++] = VarIntToDecimal(_dataStreamValues[idx], _secondaryStreamValues[idx]);
            }
        }
    }
}
