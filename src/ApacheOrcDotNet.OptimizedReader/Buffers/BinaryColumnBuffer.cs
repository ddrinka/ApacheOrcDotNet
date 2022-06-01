using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    [SkipLocalsInit]
    public class BinaryColumnBuffer : BaseColumnBuffer<byte[]>
    {
        private readonly bool[] _presentStreamBuffer;
        private readonly long[] _lengthStreamBuffer;
        private byte[] _presentInputBuffer;
        private byte[] _presentOutputBuffer;
        private byte[] _lengthInputBuffer;
        private byte[] _lengthOutputBuffer;
        private byte[] _dataInputBuffer;
        private byte[] _dataOutputBuffer;

        public BinaryColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContext context, OrcColumn column) : base(byteRangeProvider, context, column)
        {
            _presentStreamBuffer = new bool[_context.MaxValuesToRead];
            _lengthStreamBuffer = new long[_context.MaxValuesToRead];

            _presentInputBuffer = _arrayPool.Rent(_context.MaxCompressedBufferLength);
            _presentOutputBuffer = _arrayPool.Rent(_context.MaxDecompresseBufferLength);

            _lengthInputBuffer = _arrayPool.Rent(_context.MaxCompressedBufferLength);
            _lengthOutputBuffer = _arrayPool.Rent(_context.MaxDecompresseBufferLength);

            _dataInputBuffer = _arrayPool.Rent(_context.MaxCompressedBufferLength);
            _dataOutputBuffer = _arrayPool.Rent(_context.MaxDecompresseBufferLength);
        }

        public override void Fill(int stripeId, IEnumerable<StreamDetail> columnStreams, RowIndexEntry rowIndexEntry)
        {
            // Streams
            var presentStream = GetColumnStream(columnStreams, StreamKind.Present, isRequired: false);
            var lengthStream = GetColumnStream(columnStreams, StreamKind.Length);
            var dataStream = GetColumnStream(columnStreams, StreamKind.Data);

            // Stream Positions
            var presentPositions = GetPresentStreamPositions(presentStream, rowIndexEntry);
            var lengthPositions = GetTargetDataStreamPositions(presentStream, lengthStream, rowIndexEntry);
            var dataPositions = GetTargetDataStreamPositions(presentStream, dataStream, rowIndexEntry);

            // Stream Byte Ranges
            (int present, int length, int data) rangeSizes = default;
            Parallel.Invoke(
                () => GetByteRange(_presentInputBuffer, presentStream, presentPositions, ref rangeSizes.present),
                () => GetByteRange(_lengthInputBuffer, lengthStream, lengthPositions, ref rangeSizes.length),
                () => GetByteRange(_dataInputBuffer, dataStream, dataPositions, ref rangeSizes.data)
            );

            // Decompress Byte Ranges
            (int present, int length, int data) decompressedSizes = default;
            DecompressByteRange(_presentInputBuffer, _presentOutputBuffer, presentStream, presentPositions, ref decompressedSizes.present);
            DecompressByteRange(_lengthInputBuffer, _lengthOutputBuffer, lengthStream, lengthPositions, ref decompressedSizes.length);
            DecompressByteRange(_dataInputBuffer, _dataOutputBuffer, dataStream, dataPositions, ref decompressedSizes.data);

            // Parse Decompressed Bytes
            (int present, int length) valuesRead = default;
            ReadBooleanStream(_presentOutputBuffer, decompressedSizes.present, presentPositions, _presentStreamBuffer, ref valuesRead.present);
            ReadNumericStream(_lengthOutputBuffer, decompressedSizes.length, lengthPositions, isSigned: false, _lengthStreamBuffer, ref valuesRead.length);

            var dataBuffer = ResizeBuffer(_dataOutputBuffer, decompressedSizes.data, dataPositions);

            var stringOffset = 0;
            if (presentStream != null)
            {
                var lengthIndex = 0;
                for (int idx = 0; idx < valuesRead.present; idx++)
                {
                    if (_presentStreamBuffer[idx])
                    {
                        var length = (int)_lengthStreamBuffer[lengthIndex++];
                        _values[_numValuesRead++] = dataBuffer.Slice(stringOffset, length).ToArray();
                        stringOffset += length;
                    }
                    else
                        _values[_numValuesRead++] = null;
                }
            }
            else
            {
                for (int idx = 0; idx < valuesRead.length; idx++)
                {
                    var length = (int)_lengthStreamBuffer[idx];
                    _values[_numValuesRead++] = dataBuffer.Slice(stringOffset, length).ToArray();
                    stringOffset += length;
                }
            }
        }
    }
}
