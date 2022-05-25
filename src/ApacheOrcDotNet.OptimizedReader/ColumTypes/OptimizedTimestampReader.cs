using ApacheOrcDotNet.Protocol;
using System;
using System.Buffers;

namespace ApacheOrcDotNet.OptimizedReader.ColumTypes
{
    public class OptimizedTimestampReader : BaseColumnReader<DateTime?>
    {
        readonly static DateTime _orcEpoch = new DateTime(2015, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public OptimizedTimestampReader(ReaderContext readerContext) : base(readerContext)
        {
        }

        public override void FillBuffer()
        {
            var presentStream = GetStripeStream(StreamKind.Present, isRequired: false);
            var dataStream = GetStripeStream(StreamKind.Data);
            var secondaryStream = GetStripeStream(StreamKind.Secondary);

            var presentBuffer = ArrayPool<bool>.Shared.Rent(_numMaxValuesToRead);
            var dataBuffer = ArrayPool<long>.Shared.Rent(_numMaxValuesToRead);
            var secondaryBuffer = ArrayPool<long>.Shared.Rent(_numMaxValuesToRead);

            try
            {
                // Present
                var presentPositions = GetPresentStreamPositions(presentStream);
                var numPresentValuesRead = ReadBooleanStream(presentStream, presentPositions, presentBuffer.AsSpan().Slice(0, _numMaxValuesToRead));

                // Data
                var dataPostions = GetTargetedStreamPositions(presentStream, dataStream);
                var numDataValuesRead = ReadNumericStream(dataStream, dataPostions, isSigned: true, dataBuffer.AsSpan().Slice(0, _numMaxValuesToRead));

                // Secondary
                var secondaryPostions = GetTargetedStreamPositions(presentStream, secondaryStream);
                var numSecondaryValuesRead = ReadNumericStream(secondaryStream, secondaryPostions, isSigned: false, secondaryBuffer.AsSpan().Slice(0, _numMaxValuesToRead));

                if (presentStream != null)
                {
                    var secondaryIndex = 0;
                    for (int idx = 0; idx < numPresentValuesRead; idx++)
                    {
                        if (presentBuffer[idx])
                        {
                            var seconds = dataBuffer[secondaryIndex];
                            var nanosecondTicks = EncodedNanosToTicks(secondaryBuffer[secondaryIndex]);
                            var totalTicks = seconds * TimeSpan.TicksPerSecond + (seconds >= 0 ? nanosecondTicks : -nanosecondTicks);
                            _outputValuesRaw[_numValuesRead++] = _orcEpoch.AddTicks(totalTicks);
                            secondaryIndex++;
                        }
                        else
                            _outputValuesRaw[_numValuesRead++] = null;
                    }
                }
                else
                {
                    for (int idx = 0; idx < numSecondaryValuesRead; idx++)
                    {
                        var seconds = dataBuffer[idx];
                        var nanosecondTicks = EncodedNanosToTicks(secondaryBuffer[idx]);
                        var totalTicks = seconds * TimeSpan.TicksPerSecond + (seconds >= 0 ? nanosecondTicks : -nanosecondTicks);
                        _outputValuesRaw[_numValuesRead++] = _orcEpoch.AddTicks(totalTicks);
                    }
                }
            }
            finally
            {
                ArrayPool<bool>.Shared.Return(presentBuffer);
                ArrayPool<long>.Shared.Return(dataBuffer);
                ArrayPool<long>.Shared.Return(secondaryBuffer);
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
