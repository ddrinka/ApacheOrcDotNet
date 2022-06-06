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

        public BinaryColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContext context, OrcColumn column) : base(byteRangeProvider, context, column)
        {
            _presentStreamBuffer = new bool[_context.MaxValuesToRead];
            _lengthStreamBuffer = new long[_context.MaxValuesToRead];
        }

        public override async Task LoadDataAsync(int stripeId, IEnumerable<StreamDetails> columnStreams, RowIndexEntry rowIndexEntry)
        {
            LoadStreams(columnStreams, rowIndexEntry, StreamKind.Length, StreamKind.Data);

            _ = await Task.WhenAll(
                GetByteRange(_presentStreamCompressedBuffer, _presentStream, _presentStreamPositions),
                GetByteRange(_lengthStreamCompressedBuffer, _lengthStream, _lengthStreamPositions),
                GetByteRange(_dataStreamCompressedBuffer, _dataStream, _dataStreamPositions)
            );

            DecompressByteRange(_presentStreamCompressedBuffer, _presentStreamDecompressedBuffer, _presentStream, _presentStreamPositions, ref _presentStreamDecompressedBufferLength);
            DecompressByteRange(_lengthStreamCompressedBuffer, _lengthStreamDecompressedBuffer, _lengthStream, _lengthStreamPositions, ref _lengthStreamDecompressedBufferLength);
            DecompressByteRange(_dataStreamCompressedBuffer, _dataStreamDecompressedBuffer, _dataStream, _dataStreamPositions, ref _dataStreamDecompressedBufferLength);
        }

        public override void Parse()
        {
            ReadBooleanStream(_presentStreamDecompressedBuffer, _presentStreamDecompressedBufferLength, _presentStreamPositions, _presentStreamBuffer, out var presentValuesRead);
            ReadNumericStream(_lengthStreamDecompressedBuffer, _lengthStreamDecompressedBufferLength, _lengthStreamPositions, isSigned: false, _lengthStreamBuffer, out var lengthValuesRead);

            var dataBuffer = ResizeBuffer(_dataStreamDecompressedBuffer, _dataStreamDecompressedBufferLength, _dataStreamPositions);

            var stringOffset = 0;
            if (_presentStream != null)
            {
                var lengthIndex = 0;
                for (int idx = 0; idx < presentValuesRead; idx++)
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
                for (int idx = 0; idx < lengthValuesRead; idx++)
                {
                    var length = (int)_lengthStreamBuffer[idx];
                    _values[_numValuesRead++] = dataBuffer.Slice(stringOffset, length).ToArray();
                    stringOffset += length;
                }
            }
        }
    }
}
