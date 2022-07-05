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

        private ColumnDataStreams _streams;

        public BinaryColumnBuffer(IByteRangeProvider byteRangeProvider, OrcFileProperties context, OrcColumn column) : base(byteRangeProvider, context, column)
        {
            _presentStreamBuffer = new bool[_context.MaxValuesToRead];
            _lengthStreamBuffer = new long[_context.MaxValuesToRead];

            _dataStreamCompressedBuffer = _pool.Rent(_context.MaxCompressedBufferLength);
            _dataStreamDecompressedBuffer = _pool.Rent(_context.MaxDecompressedBufferLength);

            _lengthStreamCompressedBuffer = _pool.Rent(_context.MaxCompressedBufferLength);
            _lengthStreamDecompressedBuffer = _pool.Rent(_context.MaxDecompressedBufferLength);

            _presentStreamCompressedBuffer = _pool.Rent(_context.MaxCompressedBufferLength);
            _presentStreamDecompressedBuffer = _pool.Rent(_context.MaxDecompressedBufferLength);
        }

        public override async Task LoadDataAsync(int stripeId, ColumnDataStreams streams)
        {
            _streams = streams;

            _ = await Task.WhenAll(
                GetByteRangeAsync(_streams.Present, _presentStreamCompressedBuffer),
                GetByteRangeAsync(_streams.Length, _lengthStreamCompressedBuffer),
                GetByteRangeAsync(_streams.Data, _dataStreamCompressedBuffer)
            );

            DecompressByteRange(_streams.Present, _presentStreamCompressedBuffer, _presentStreamDecompressedBuffer, ref _presentStreamDecompressedBufferLength);
            DecompressByteRange(_streams.Length, _lengthStreamCompressedBuffer, _lengthStreamDecompressedBuffer, ref _lengthStreamDecompressedBufferLength);
            DecompressByteRange(_streams.Data, _dataStreamCompressedBuffer, _dataStreamDecompressedBuffer, ref _dataStreamDecompressedBufferLength);
        }

        public override void Fill()
        {
            ReadBooleanStream(_streams.Present, _presentStreamDecompressedBuffer, _presentStreamDecompressedBufferLength, _presentStreamBuffer, out var presentValuesRead);
            ReadNumericStream(_streams.Length, _lengthStreamDecompressedBuffer, _lengthStreamDecompressedBufferLength, isSigned: false, _lengthStreamBuffer, out var lengthValuesRead);

            var dataBuffer = GetDataStream(_streams.Data, _dataStreamDecompressedBuffer, _dataStreamDecompressedBufferLength);

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
