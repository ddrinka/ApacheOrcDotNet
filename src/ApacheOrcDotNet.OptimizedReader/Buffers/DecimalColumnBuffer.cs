using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System.Collections.Generic;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    [SkipLocalsInit]
    public class DecimalColumnBuffer : BaseColumnBuffer<decimal?>
    {
        private bool[] _presentStreamBuffer;
        private BigInteger[] _dataStreamBuffer;
        private long[] _secondaryStreamBuffer;
        private byte[] _presentInputBuffer;
        private byte[] _presentOutputBuffer;
        private byte[] _dataInputBuffer;
        private byte[] _dataOutputBuffer;
        private byte[] _secondaryInputBuffer;
        private byte[] _secondaryOutputBuffer;

        public DecimalColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContext context, OrcColumn column) : base(byteRangeProvider, context, column)
        {
            _presentStreamBuffer = new bool[_context.MaxValuesToRead];
            _dataStreamBuffer = new BigInteger[_context.MaxValuesToRead];
            _secondaryStreamBuffer = new long[_context.MaxValuesToRead];

            _presentInputBuffer = _arrayPool.Rent(_context.MaxCompressedBufferLength);
            _presentOutputBuffer = _arrayPool.Rent(_context.MaxDecompresseBufferLength);

            _dataInputBuffer = _arrayPool.Rent(_context.MaxCompressedBufferLength);
            _dataOutputBuffer = _arrayPool.Rent(_context.MaxDecompresseBufferLength);

            _secondaryInputBuffer = _arrayPool.Rent(_context.MaxCompressedBufferLength);
            _secondaryOutputBuffer = _arrayPool.Rent(_context.MaxDecompresseBufferLength);
        }

        public override void Fill(int stripeId, IEnumerable<StreamDetail> columnStreams, RowIndexEntry rowIndexEntry)
        {
            // Streams
            var presentStream = GetColumnStream(columnStreams, StreamKind.Present, isRequired: false);
            var dataStream = GetColumnStream(columnStreams, StreamKind.Data);
            var secondaryStream = GetColumnStream(columnStreams, StreamKind.Secondary);

            // Stream Positions
            var presentPositions = GetPresentStreamPositions(presentStream, rowIndexEntry);
            var dataPositions = GetTargetDataStreamPositions(presentStream, dataStream, rowIndexEntry);
            var secondaryPostions = GetTargetDataStreamPositions(presentStream, secondaryStream, rowIndexEntry);

            // Stream Byte Ranges
            (int present, int data, int secondary) rangeSizes = default;
            Parallel.Invoke(
                () => GetByteRange(_presentInputBuffer, presentStream, presentPositions, ref rangeSizes.present),
                () => GetByteRange(_dataInputBuffer, dataStream, dataPositions, ref rangeSizes.data),
                () => GetByteRange(_secondaryInputBuffer, secondaryStream, secondaryPostions, ref rangeSizes.secondary)
            );

            // Decompress Byte Ranges
            (int present, int data, int secondary) decompressedSizes = default;
            Parallel.Invoke(
                () => DecompressByteRange(_presentInputBuffer, _presentOutputBuffer, presentStream, presentPositions, ref decompressedSizes.present),
                () => DecompressByteRange(_dataInputBuffer, _dataOutputBuffer, dataStream, dataPositions, ref decompressedSizes.data),
                () => DecompressByteRange(_secondaryInputBuffer, _secondaryOutputBuffer, secondaryStream, secondaryPostions, ref decompressedSizes.secondary)
            );

            // Parse Decompressed Bytes
            (int present, int data, int secondary) valuesRead = default;
            Parallel.Invoke(
                () => ReadBooleanStream(_presentOutputBuffer, decompressedSizes.present, presentPositions, _presentStreamBuffer, ref valuesRead.present),
                () => ReadVarIntStream(_dataOutputBuffer, decompressedSizes.data, dataPositions, _dataStreamBuffer, ref valuesRead.data),
                () => ReadNumericStream(_secondaryOutputBuffer, decompressedSizes.secondary, secondaryPostions, isSigned: true, _secondaryStreamBuffer, ref valuesRead.secondary)
            );

            var secondaryIndex = 0;
            if (presentStream != null)
            {
                for (int idx = 0; idx < valuesRead.present; idx++)
                {
                    if (_presentStreamBuffer[idx])
                    {
                        _values[_numValuesRead++] = BigIntegerToDecimal(_dataStreamBuffer[secondaryIndex], _secondaryStreamBuffer[secondaryIndex]);
                        secondaryIndex++;
                    }
                    else
                        _values[_numValuesRead++] = null;
                }
            }
            else
            {
                for (int idx = 0; idx < valuesRead.secondary; idx++)
                    _values[_numValuesRead++] = BigIntegerToDecimal(_dataStreamBuffer[idx], _secondaryStreamBuffer[idx]);
            }
        }
    }
}
