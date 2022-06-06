using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    [SkipLocalsInit]
    public class DecimalAsDoubleColumnBuffer : BaseColumnBuffer<double>
    {
        private bool[] _presentStreamValues;
        private BigInteger[] _dataStreamValues;
        private long[] _secondaryStreamValues;

        public DecimalAsDoubleColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContext context, OrcColumn column) : base(byteRangeProvider, context, column)
        {
            _presentStreamValues = new bool[_context.MaxValuesToRead];
            _dataStreamValues = new BigInteger[_context.MaxValuesToRead];
            _secondaryStreamValues = new long[_context.MaxValuesToRead];
        }

        public override async Task LoadDataAsync(int stripeId, IEnumerable<StreamDetails> columnStreams, RowIndexEntry rowIndexEntry)
        {
            LoadStreams(columnStreams, rowIndexEntry, StreamKind.Data, StreamKind.Secondary);

            _ = await Task.WhenAll(
                GetByteRange(_presentStreamCompressedBuffer, _presentStream, _presentStreamPositions),
                GetByteRange(_dataStreamCompressedBuffer, _dataStream, _dataStreamPositions),
                GetByteRange(_secondaryStreamCompressedBuffer, _secondaryStream, _secondaryStreamPositions)
            );

            DecompressByteRange(_presentStreamCompressedBuffer, _presentStreamDecompressedBuffer, _presentStream, _presentStreamPositions, ref _presentStreamDecompressedBufferLength);
            DecompressByteRange(_dataStreamCompressedBuffer, _dataStreamDecompressedBuffer, _dataStream, _dataStreamPositions, ref _dataStreamDecompressedBufferLength);
            DecompressByteRange(_secondaryStreamCompressedBuffer, _secondaryStreamDecompressedBuffer, _secondaryStream, _secondaryStreamPositions, ref _secondaryStreamDecompressedBufferLength);
        }

        public override void Parse()
        {
            ReadBooleanStream(_presentStreamDecompressedBuffer, _presentStreamDecompressedBufferLength, _presentStreamPositions, _presentStreamValues, out var presentValuesRead);
            ReadVarIntStream(_dataStreamDecompressedBuffer, _dataStreamDecompressedBufferLength, _dataStreamPositions, _dataStreamValues, out var dataValuesRead);
            ReadNumericStream(_secondaryStreamDecompressedBuffer, _secondaryStreamDecompressedBufferLength, _secondaryStreamPositions, isSigned: true, _secondaryStreamValues, out var secondaryValuesRead);

            if (dataValuesRead != secondaryValuesRead)
                throw new InvalidOperationException($"Number of data({dataValuesRead}) and secondary({secondaryValuesRead}) values must match.");

            var secondaryIndex = 0;
            if (_presentStream != null)
            {
                for (int idx = 0; idx < presentValuesRead; idx++)
                {
                    if (_presentStreamValues[idx])
                    {
                        _values[_numValuesRead++] = BigIntegerToDouble(_dataStreamValues[secondaryIndex], _secondaryStreamValues[secondaryIndex]);
                        secondaryIndex++;
                    }
                    else
                        _values[_numValuesRead++] = double.NaN;
                }
            }
            else
            {
                for (int idx = 0; idx < dataValuesRead; idx++)
                    _values[_numValuesRead++] = BigIntegerToDouble(_dataStreamValues[idx], _secondaryStreamValues[idx]);
            }
        }
    }
}
