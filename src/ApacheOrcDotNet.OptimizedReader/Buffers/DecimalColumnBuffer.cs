﻿using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using System;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    public class DecimalColumnBuffer : BaseColumnBuffer<decimal?>
    {
        private readonly byte[] _presentRleBuffer = new byte[Constants.RleBufferMaxLength];
        private readonly long[] _secondaryRleBuffer = new long[Constants.RleBufferMaxLength];

        private readonly bool[] _presentStreamValues;
        private readonly long[] _dataStreamValues;
        private readonly long[] _secondaryStreamValues;

        private byte[] _dataStreamCompressedBuffer;
        private byte[] _dataStreamDecompressedBuffer;
        private int _dataStreamDecompressedBufferLength;

        private byte[] _presentStreamCompressedBuffer;
        private byte[] _presentStreamDecompressedBuffer;
        private int _presentStreamDecompressedBufferLength;

        private byte[] _secondaryStreamCompressedBuffer;
        private byte[] _secondaryStreamDecompressedBuffer;
        private int _secondaryStreamDecompressedBufferLength;

        public DecimalColumnBuffer(IByteRangeProvider byteRangeProvider, OrcFileProperties orcFileProperties, OrcColumn column) : base(byteRangeProvider, orcFileProperties, column)
        {
            _presentStreamValues = new bool[orcFileProperties.MaxValuesToRead];
            _dataStreamValues = new long[orcFileProperties.MaxValuesToRead];
            _secondaryStreamValues = new long[orcFileProperties.MaxValuesToRead];

            _dataStreamCompressedBuffer = new byte[orcFileProperties.ReusableBufferLength];
            _dataStreamDecompressedBuffer = new byte[orcFileProperties.ReusableBufferLength];

            _presentStreamCompressedBuffer = new byte[orcFileProperties.ReusableBufferLength];
            _presentStreamDecompressedBuffer = new byte[orcFileProperties.ReusableBufferLength];

            _secondaryStreamCompressedBuffer = new byte[orcFileProperties.ReusableBufferLength];
            _secondaryStreamDecompressedBuffer = new byte[orcFileProperties.ReusableBufferLength];
        }

        public override async Task LoadDataAsync(int stripeId, ColumnDataStreams streams)
        {
            CheckByteRangeBufferLength(streams.Present, ref _presentStreamCompressedBuffer);
            CheckByteRangeBufferLength(streams.Data, ref _dataStreamCompressedBuffer);
            CheckByteRangeBufferLength(streams.Secondary, ref _secondaryStreamCompressedBuffer);

            await Task.WhenAll(
                GetByteRangeAsync(streams.Present, _presentStreamCompressedBuffer),
                GetByteRangeAsync(streams.Data, _dataStreamCompressedBuffer),
                GetByteRangeAsync(streams.Secondary, _secondaryStreamCompressedBuffer)
            );

            DecompressByteRange(streams.Present, _presentStreamCompressedBuffer, ref _presentStreamDecompressedBuffer, ref _presentStreamDecompressedBufferLength);
            DecompressByteRange(streams.Data, _dataStreamCompressedBuffer, ref _dataStreamDecompressedBuffer, ref _dataStreamDecompressedBufferLength);
            DecompressByteRange(streams.Secondary, _secondaryStreamCompressedBuffer, ref _secondaryStreamDecompressedBuffer, ref _secondaryStreamDecompressedBufferLength);

            Fill(streams);
        }

        private void Fill(ColumnDataStreams streams)
        {
            ReadBooleanStream(streams.Present, _presentStreamDecompressedBuffer.AsSpan()[.._presentStreamDecompressedBufferLength], _presentRleBuffer, _presentStreamValues, out var presentValuesRead);
            ReadVarIntStream(streams.Data, _dataStreamDecompressedBuffer.AsSpan()[.._dataStreamDecompressedBufferLength], _dataStreamValues, out var dataValuesRead);
            ReadNumericStream(streams.Secondary, _secondaryStreamDecompressedBuffer.AsSpan()[.._secondaryStreamDecompressedBufferLength], _secondaryRleBuffer, isSigned: true, _secondaryStreamValues, out var secondaryValuesRead);

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
