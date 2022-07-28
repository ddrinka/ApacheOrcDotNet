using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using System;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    public class TimestampColumnBuffer : BaseColumnBuffer<DateTime?>
    {
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

        public TimestampColumnBuffer(IByteRangeProvider byteRangeProvider, OrcFileProperties orcFileProperties, OrcColumn column) : base(byteRangeProvider, orcFileProperties, column)
        {
            _presentStreamValues = new bool[orcFileProperties.MaxValuesToRead];
            _dataStreamValues = new long[orcFileProperties.MaxValuesToRead];
            _secondaryStreamValues = new long[orcFileProperties.MaxValuesToRead];

            _dataStreamCompressedBuffer = new byte[orcFileProperties.ReusableBufferLength];
            _dataStreamDecompressedBuffer = new byte[orcFileProperties.ReusableBufferLength];

            _presentStreamCompressedBuffer = new byte[orcFileProperties.ReusableBufferLength];
            _presentStreamDecompressedBuffer = new byte[orcFileProperties.ReusableBufferLength];

            _secondaryStreamCompressedBuffer = new byte[orcFileProperties.ReusableBufferLength];
            _secondaryStreamDecompressedBuffer = new byte[orcFileProperties.ReusableBufferLength];
        }

        public override async Task LoadDataAsync(int stripeId, ColumnDataStreams streams)
        {
            _ = await Task.WhenAll(
                GetByteRangeAsync(streams.Present, _presentStreamCompressedBuffer),
                GetByteRangeAsync(streams.Data, _dataStreamCompressedBuffer),
                GetByteRangeAsync(streams.Secondary, _secondaryStreamCompressedBuffer)
            );

            DecompressByteRange(streams.Present, _presentStreamCompressedBuffer, _presentStreamDecompressedBuffer, ref _presentStreamDecompressedBufferLength);
            DecompressByteRange(streams.Data, _dataStreamCompressedBuffer, _dataStreamDecompressedBuffer, ref _dataStreamDecompressedBufferLength);
            DecompressByteRange(streams.Secondary, _secondaryStreamCompressedBuffer, _secondaryStreamDecompressedBuffer, ref _secondaryStreamDecompressedBufferLength);

            Fill(streams);
        }

        private void Fill(ColumnDataStreams streams)
        {
            ReadBooleanStream(streams.Present, _presentStreamDecompressedBuffer[.._presentStreamDecompressedBufferLength], _presentStreamValues, out var presentValuesRead);
            ReadNumericStream(streams.Data, _dataStreamDecompressedBuffer[.._dataStreamDecompressedBufferLength], isSigned: true, _dataStreamValues, out var dataValuesRead);
            ReadNumericStream(streams.Secondary, _secondaryStreamDecompressedBuffer[.._secondaryStreamDecompressedBufferLength], isSigned: false, _secondaryStreamValues, out var secondaryValuesRead);

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
                        _values[_numValuesRead++] = Constants.OrcEpoch.AddTicks(totalTicks);
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
                    _values[_numValuesRead++] = Constants.OrcEpoch.AddTicks(totalTicks);
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
