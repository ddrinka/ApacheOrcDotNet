using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    [SkipLocalsInit]
    public class DecimalColumnBuffer : BaseColumnBuffer<decimal?>
    {
        private bool[] _presentStreamValues;
        private BigInteger[] _dataStreamValues;
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

        public DecimalColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContext context, OrcColumn column) : base(byteRangeProvider, context, column)
        {
            _presentStreamValues = new bool[_context.MaxValuesToRead];
            _dataStreamValues = new BigInteger[_context.MaxValuesToRead];
            _secondaryStreamValues = new long[_context.MaxValuesToRead];

            _dataStreamCompressedBuffer = _pool.Rent(_context.MaxCompressedBufferLength);
            _dataStreamDecompressedBuffer = _pool.Rent(_context.MaxDecompresseBufferLength);

            _presentStreamCompressedBuffer = _pool.Rent(_context.MaxCompressedBufferLength);
            _presentStreamDecompressedBuffer = _pool.Rent(_context.MaxDecompresseBufferLength);

            _secondaryStreamCompressedBuffer = _pool.Rent(_context.MaxCompressedBufferLength);
            _secondaryStreamDecompressedBuffer = _pool.Rent(_context.MaxDecompresseBufferLength);
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
                        _values[_numValuesRead++] = BigIntegerToDecimal(_dataStreamValues[valueIndex], _secondaryStreamValues[valueIndex]);
                        valueIndex++;
                    }
                    else
                        _values[_numValuesRead++] = null;
                }
            }
            else
            {
                for (int idx = 0; idx < secondaryValuesRead; idx++)
                    _values[_numValuesRead++] = BigIntegerToDecimal(_dataStreamValues[idx], _secondaryStreamValues[idx]);
            }
        }
    }
}
