using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using System;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    public class TimestampColumnBuffer : BaseColumnBuffer<DateTime?>
    {
        private readonly static DateTime _orcEpoch = new DateTime(2015, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private readonly bool[] _presentStreamValues;
        private readonly long[] _dataStreamValues;

        private byte[] _dataStreamCompressedBuffer;
        private byte[] _dataStreamDecompressedBuffer;
        private int _dataStreamDecompressedBufferLength;

        private byte[] _presentStreamCompressedBuffer;
        private byte[] _presentStreamDecompressedBuffer;
        private int _presentStreamDecompressedBufferLength;

        private byte[] _secondaryStreamCompressedBuffer;
        private byte[] _secondaryStreamDecompressedBuffer;
        private int _secondaryStreamDecompressedBufferLength;

        private long[] _secondaryStreamValues;

        private ColumnDataStreams _streams;

        public TimestampColumnBuffer(IByteRangeProvider byteRangeProvider, OrcFileProperties orcFileProperties, OrcColumn column) : base(byteRangeProvider, orcFileProperties, column)
        {
            _presentStreamValues = new bool[_orcFileProperties.MaxValuesToRead];
            _dataStreamValues = new long[_orcFileProperties.MaxValuesToRead];
            _secondaryStreamValues = new long[_orcFileProperties.MaxValuesToRead];

            _dataStreamCompressedBuffer = _pool.Rent(_orcFileProperties.MaxCompressedBufferLength);
            _dataStreamDecompressedBuffer = _pool.Rent(_orcFileProperties.MaxDecompressedBufferLength);

            _presentStreamCompressedBuffer = _pool.Rent(_orcFileProperties.MaxCompressedBufferLength);
            _presentStreamDecompressedBuffer = _pool.Rent(_orcFileProperties.MaxDecompressedBufferLength);

            _secondaryStreamCompressedBuffer = _pool.Rent(_orcFileProperties.MaxCompressedBufferLength);
            _secondaryStreamDecompressedBuffer = _pool.Rent(_orcFileProperties.MaxDecompressedBufferLength);
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
            ReadNumericStream(_streams.Data, _dataStreamDecompressedBuffer, _dataStreamDecompressedBufferLength, isSigned: true, _dataStreamValues, out var dataValuesRead);
            ReadNumericStream(_streams.Secondary, _secondaryStreamDecompressedBuffer, _secondaryStreamDecompressedBufferLength, isSigned: false, _secondaryStreamValues, out var secondaryValuesRead);

            if (dataValuesRead != secondaryValuesRead)
                throw new InvalidOperationException($"Number of data({dataValuesRead}) and secondary({secondaryValuesRead}) values must match.");

            if (presentValuesRead > 0)
            {
                var valueIndex = 0;
                for (int idx = 0; idx < presentValuesRead; idx++)
                {
                    if (_presentStreamValues[idx])
                    {
                        var seconds = _dataStreamValues[valueIndex];
                        var nanosecondTicks = EncodedNanosToTicks(_secondaryStreamValues[valueIndex]);
                        var totalTicks = seconds * TimeSpan.TicksPerSecond + (seconds >= 0 ? nanosecondTicks : -nanosecondTicks);
                        _values[_numValuesRead++] = _orcEpoch.AddTicks(totalTicks);
                        valueIndex++;
                    }
                    else
                        _values[_numValuesRead++] = null;
                }
            }
            else
            {
                for (int idx = 0; idx < dataValuesRead; idx++)
                {
                    var seconds = _dataStreamValues[idx];
                    var nanosecondTicks = EncodedNanosToTicks(_secondaryStreamValues[idx]);
                    var totalTicks = seconds * TimeSpan.TicksPerSecond + (seconds >= 0 ? nanosecondTicks : -nanosecondTicks);
                    _values[_numValuesRead++] = _orcEpoch.AddTicks(totalTicks);
                }
            }
        }

        private long EncodedNanosToTicks(long encodedNanos)
        {
            var scale = (int)(encodedNanos & 0x7);
            var nanos = encodedNanos >> 3;

            if (scale == 0)
                return nanos;

            while (scale-- >= 0)
                nanos *= 10;

            return nanos / 100;     //100 nanoseconds per tick
        }
    }
}
