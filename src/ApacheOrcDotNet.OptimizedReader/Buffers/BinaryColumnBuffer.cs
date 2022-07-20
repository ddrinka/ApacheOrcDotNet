using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    public class BinaryColumnBuffer : BaseColumnBuffer<byte[]>
    {
        private readonly bool[] _presentStreamBuffer;
        private readonly long[] _lengthStreamBuffer;

        private byte[] _dataStreamCompressedBuffer;
        private byte[] _dataStreamDecompressedBuffer;
        private int _dataStreamDecompressedBufferLength;

        private byte[] _lengthStreamCompressedBuffer;
        private byte[] _lengthStreamDecompressedBuffer;
        private int _lengthStreamDecompressedBufferLength;

        private byte[] _presentStreamCompressedBuffer;
        private byte[] _presentStreamDecompressedBuffer;
        private int _presentStreamDecompressedBufferLength;

        public BinaryColumnBuffer(IByteRangeProvider byteRangeProvider, OrcFileProperties orcFileProperties, OrcColumn column) : base(byteRangeProvider, orcFileProperties, column)
        {
            _presentStreamBuffer = new bool[orcFileProperties.MaxValuesToRead];
            _lengthStreamBuffer = new long[orcFileProperties.MaxValuesToRead];

            _dataStreamCompressedBuffer = new byte[orcFileProperties.ReusableBufferLength];
            _dataStreamDecompressedBuffer = new byte[orcFileProperties.ReusableBufferLength];

            _lengthStreamCompressedBuffer = new byte[orcFileProperties.ReusableBufferLength];
            _lengthStreamDecompressedBuffer = new byte[orcFileProperties.ReusableBufferLength];

            _presentStreamCompressedBuffer = new byte[orcFileProperties.ReusableBufferLength];
            _presentStreamDecompressedBuffer = new byte[orcFileProperties.ReusableBufferLength];
        }

        public override async Task LoadDataAsync(int stripeId, ColumnDataStreams streams)
        {
            _ = await Task.WhenAll(
                GetByteRangeAsync(streams.Present, _presentStreamCompressedBuffer),
                GetByteRangeAsync(streams.Length, _lengthStreamCompressedBuffer),
                GetByteRangeAsync(streams.Data, _dataStreamCompressedBuffer)
            );

            DecompressByteRange(streams.Present, _presentStreamCompressedBuffer, _presentStreamDecompressedBuffer, ref _presentStreamDecompressedBufferLength);
            DecompressByteRange(streams.Length, _lengthStreamCompressedBuffer, _lengthStreamDecompressedBuffer, ref _lengthStreamDecompressedBufferLength);
            DecompressByteRange(streams.Data, _dataStreamCompressedBuffer, _dataStreamDecompressedBuffer, ref _dataStreamDecompressedBufferLength);

            Fill(streams);
        }

        private void Fill(ColumnDataStreams streams)
        {
            ReadBooleanStream(streams.Present, _presentStreamDecompressedBuffer, _presentStreamDecompressedBufferLength, _presentStreamBuffer, out var presentValuesRead);
            ReadNumericStream(streams.Length, _lengthStreamDecompressedBuffer, _lengthStreamDecompressedBufferLength, isSigned: false, _lengthStreamBuffer, out var lengthValuesRead);

            var dataBuffer = GetDataStream(streams.Data, _dataStreamDecompressedBuffer, _dataStreamDecompressedBufferLength);

            var stringOffset = 0;
            if (presentValuesRead > 0)
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
