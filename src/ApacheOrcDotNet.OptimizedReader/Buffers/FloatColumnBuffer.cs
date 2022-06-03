using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    [SkipLocalsInit]
    public class FloatColumnBuffer : BaseColumnBuffer<float>
    {
        private bool[] _presentStreamBuffer;
        private byte[] _presentInputBuffer;
        private byte[] _presentOutputBuffer;
        private byte[] _dataInputBuffer;
        private byte[] _dataOutputBuffer;
        private byte[] _valueBuffer;

        public FloatColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContext context, OrcColumn column) : base(byteRangeProvider, context, column)
        {
            _presentStreamBuffer = new bool[_context.MaxValuesToRead];

            _presentInputBuffer = _arrayPool.Rent(_context.MaxCompressedBufferLength);
            _presentOutputBuffer = _arrayPool.Rent(_context.MaxDecompresseBufferLength);

            _dataInputBuffer = _arrayPool.Rent(_context.MaxCompressedBufferLength);
            _dataOutputBuffer = _arrayPool.Rent(_context.MaxDecompresseBufferLength);

            _valueBuffer = new byte[4];
        }

        public override void Fill(int stripeId, IEnumerable<StreamDetail> columnStreams, RowIndexEntry rowIndexEntry)
        {
            // Streams
            var presentStream = GetColumnStream(columnStreams, StreamKind.Present, isRequired: false);
            var dataStream = GetColumnStream(columnStreams, StreamKind.Data);

            // Stream Positions
            var presentPositions = GetPresentStreamPositions(presentStream, rowIndexEntry);
            var dataPositions = GetTargetDataStreamPositions(presentStream, dataStream, rowIndexEntry);

            // Stream Byte Ranges
            (int present, int data) rangeSizes = default;
            Parallel.Invoke(
                () => GetByteRange(_presentInputBuffer, presentStream, presentPositions, ref rangeSizes.present),
                () => GetByteRange(_dataInputBuffer, dataStream, dataPositions, ref rangeSizes.data)
            );

            // Decompress Byte Ranges
            (int present, int data) decompressedSizes = default;
            DecompressByteRange(_presentInputBuffer, _presentOutputBuffer, presentStream, presentPositions, ref decompressedSizes.present);
            DecompressByteRange(_dataInputBuffer, _dataOutputBuffer, dataStream, dataPositions, ref decompressedSizes.data);

            // Parse Decompressed Bytes
            int presentValuesRead = default;
            ReadBooleanStream(_presentOutputBuffer, decompressedSizes.present, presentPositions, _presentStreamBuffer, ref presentValuesRead);

            var dataReader = new BufferReader(ResizeBuffer(_dataOutputBuffer, decompressedSizes.data, dataPositions));

            if (presentStream != null)
            {
                for (int idx = 0; idx < presentValuesRead; idx++)
                {
                    if (_presentStreamBuffer[idx])
                    {
                        if (!dataReader.TryCopyTo(_valueBuffer))
                            throw new InvalidOperationException("Read past end of stream");

                        _values[_numValuesRead++] = BitConverter.ToSingle(_valueBuffer);
                    }
                    else
                        _values[_numValuesRead++] = float.NaN;
                }
            }
            else
            {
                while (dataReader.TryCopyTo(_valueBuffer))
                {
                    _values[_numValuesRead++] = BitConverter.ToSingle(_valueBuffer);

                    if (_numValuesRead >= _values.Length)
                        break;
                }
            }
        }
    }
}