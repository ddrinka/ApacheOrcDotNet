using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    [SkipLocalsInit]
    public class DoubleColumnBuffer : BaseColumnBuffer<double>
    {
        private bool[] _presentStreamValues;
        private byte[] _valueBuffer;

        public DoubleColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContext context, OrcColumn column) : base(byteRangeProvider, context, column)
        {
            _presentStreamValues = new bool[_context.MaxValuesToRead];
            _valueBuffer = new byte[8];
        }

        public override async Task LoadDataAsync(int stripeId, IEnumerable<StreamDetails> columnStreams, RowIndexEntry rowIndexEntry)
        {
            LoadStreams(columnStreams, rowIndexEntry, StreamKind.Data);

            _ = await Task.WhenAll(
                GetByteRange(_presentStreamCompressedBuffer, _presentStream, _presentStreamPositions),
                GetByteRange(_dataStreamCompressedBuffer, _dataStream, _dataStreamPositions)
            );

            DecompressByteRange(_presentStreamCompressedBuffer, _presentStreamDecompressedBuffer, _presentStream, _presentStreamPositions, ref _presentStreamDecompressedBufferLength);
            DecompressByteRange(_dataStreamCompressedBuffer, _dataStreamDecompressedBuffer, _dataStream, _dataStreamPositions, ref _dataStreamDecompressedBufferLength);
        }

        public override void Parse()
        {
            ReadBooleanStream(_presentStreamDecompressedBuffer, _presentStreamDecompressedBufferLength, _presentStreamPositions, _presentStreamValues, out var presentValuesRead);

            var dataReader = new BufferReader(ResizeBuffer(_dataStreamDecompressedBuffer, _dataStreamDecompressedBufferLength, _dataStreamPositions));

            if (_presentStream != null)
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