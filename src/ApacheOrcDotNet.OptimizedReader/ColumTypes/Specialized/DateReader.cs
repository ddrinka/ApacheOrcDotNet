using ApacheOrcDotNet.Protocol;
using System;
using System.Buffers;

namespace ApacheOrcDotNet.OptimizedReader.ColumTypes.Specialized
{
    public class DateTimeReader : BaseColumnReader<DateTime?>
    {
        readonly static DateTime _unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public DateTimeReader(ReaderContext readerContext) : base(readerContext)
        {
        }

        public override void FillBuffer()
        {
            var presentStreamRequired = _readerContext.RowIndexEntry.Statistics.HasNull;

            var presentStream = GetStripeStream(StreamKind.Present, presentStreamRequired);
            var dataStream = GetStripeStream(StreamKind.Data);

            var presentBuffer = ArrayPool<bool>.Shared.Rent(_numMaxValuesToRead);
            var dataBuffer = ArrayPool<long>.Shared.Rent(_numMaxValuesToRead);

            try
            {
                // Present
                var presentPositions = GetPresentStreamPositions(presentStream);
                var numPresentValuesRead = ReadBooleanStream(presentStream, presentPositions, presentBuffer.AsSpan().Slice(0, _numMaxValuesToRead));

                // Data
                var dataPostions = GetTargetedStreamPositions(presentStream, dataStream);
                var numDataValuesRead = ReadNumericStream(dataStream, dataPostions, isSigned: true, dataBuffer.AsSpan().Slice(0, _numMaxValuesToRead));

                if (presentStreamRequired)
                {
                    var dataIndex = 0;
                    for (int idx = 0; idx < numPresentValuesRead; idx++)
                    {
                        if (presentBuffer[idx])
                            _outputValuesRaw[_numValuesRead++] = _unixEpoch.AddTicks(dataBuffer[dataIndex++] * TimeSpan.TicksPerDay);
                        else
                            _outputValuesRaw[_numValuesRead++] = null;
                    }
                }
                else
                {
                    for (int idx = 0; idx < numDataValuesRead; idx++)
                        _outputValuesRaw[_numValuesRead++] = _unixEpoch.AddTicks(dataBuffer[idx] * TimeSpan.TicksPerDay);
                }
            }
            finally
            {
                ArrayPool<bool>.Shared.Return(presentBuffer);
                ArrayPool<long>.Shared.Return(dataBuffer);
            }
        }
    }
}