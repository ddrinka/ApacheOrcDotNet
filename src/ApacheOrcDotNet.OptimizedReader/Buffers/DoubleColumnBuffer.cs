﻿using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    [SkipLocalsInit]
    public class DoubleColumnBuffer : BaseColumnBuffer<double>
    {
        private bool[] _presentStreamValues;
        private byte[] _valueBuffer;

        private byte[] _dataStreamCompressedBuffer;
        private byte[] _dataStreamDecompressedBuffer;
        private int _dataStreamDecompressedBufferLength;

        private byte[] _presentStreamCompressedBuffer;
        private byte[] _presentStreamDecompressedBuffer;
        private int _presentStreamDecompressedBufferLength;

        private ColumnDataStreams _streams;

        public DoubleColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContext context, OrcColumn column) : base(byteRangeProvider, context, column)
        {
            _presentStreamValues = new bool[_context.MaxValuesToRead];
            _valueBuffer = new byte[8];

            _dataStreamCompressedBuffer = _pool.Rent(_context.MaxCompressedBufferLength);
            _dataStreamDecompressedBuffer = _pool.Rent(_context.MaxDecompresseBufferLength);

            _presentStreamCompressedBuffer = _pool.Rent(_context.MaxCompressedBufferLength);
            _presentStreamDecompressedBuffer = _pool.Rent(_context.MaxDecompresseBufferLength);
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

            var dataReader = new BufferReader(ResizeBuffer(_streams.Data, _dataStreamDecompressedBuffer, _dataStreamDecompressedBufferLength));

            if (presentValuesRead > 0)
            {
                for (int idx = 0; idx < presentValuesRead; idx++)
                {
                    if (_presentStreamValues[idx])
                    {
                        if (!dataReader.TryCopyTo(_valueBuffer))
                            throw new InvalidOperationException("Read past end of stream");

                        _values[_numValuesRead++] = BitConverter.ToDouble(_valueBuffer);
                    }
                    else
                        _values[_numValuesRead++] = double.NaN;
                }
            }
            else
            {
                while (dataReader.TryCopyTo(_valueBuffer))
                {
                    _values[_numValuesRead++] = BitConverter.ToDouble(_valueBuffer);

                    if (_numValuesRead >= _values.Length)
                        break;
                }
            }
        }
    }
}