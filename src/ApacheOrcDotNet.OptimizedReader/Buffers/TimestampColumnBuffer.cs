using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    [SkipLocalsInit]
    public class TimestampColumnBuffer : BaseColumnBuffer<DateTime?>
    {
        readonly static DateTime _orcEpoch = new DateTime(2015, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private readonly bool[] _presentStreamValues;
        private readonly long[] _dataStreamValues;
        private long[] _secondaryStreamValues;

        public TimestampColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContext context, OrcColumn column) : base(byteRangeProvider, context, column)
        {
            _presentStreamValues = new bool[_context.MaxValuesToRead];
            _dataStreamValues = new long[_context.MaxValuesToRead];
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
            ReadNumericStream(_dataStreamDecompressedBuffer, _dataStreamDecompressedBufferLength, _dataStreamPositions, isSigned: true, _dataStreamValues, out var dataValuesRead);
            ReadNumericStream(_secondaryStreamDecompressedBuffer, _secondaryStreamDecompressedBufferLength, _secondaryStreamPositions, isSigned: false, _secondaryStreamValues, out var secondaryValuesRead);

            if (dataValuesRead != secondaryValuesRead)
                throw new InvalidOperationException($"Number of data({dataValuesRead}) and secondary({secondaryValuesRead}) values must match.");

            if (_presentStream != null)
            {
                var secondaryIndex = 0;
                for (int idx = 0; idx < presentValuesRead; idx++)
                {
                    if (_presentStreamValues[idx])
                    {
                        var seconds = _dataStreamValues[secondaryIndex];
                        var nanosecondTicks = EncodedNanosToTicks(_secondaryStreamValues[secondaryIndex]);
                        var totalTicks = seconds * TimeSpan.TicksPerSecond + (seconds >= 0 ? nanosecondTicks : -nanosecondTicks);
                        _values[_numValuesRead++] = _orcEpoch.AddTicks(totalTicks);
                        secondaryIndex++;
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
