using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Buffers
{
    [SkipLocalsInit]
    public class DateColumnBuffer : BaseColumnBuffer<DateTime?>
    {
        readonly static DateTime _unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private readonly bool[] _presentStreamValues;
        private readonly long[] _dataStreamValues;

        public DateColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContext context, OrcColumn column) : base(byteRangeProvider, context, column)
        {
            _presentStreamValues = new bool[_context.MaxValuesToRead];
            _dataStreamValues = new long[_context.MaxValuesToRead];
        }

        public override async Task LoadDataAsync(int stripeId, IEnumerable<StreamDetails> columnStreams, RowIndexEntry rowIndexEntry)
        {
            LoadStreams(columnStreams, rowIndexEntry, StreamKind.Data);

            _ = await Task.WhenAll(
                GetByteRange(_presentStreamCompressedBuffer, _presentStream, _presentStreamPositions),
                GetByteRange(_dataStreamCompressedBuffer, _dataStream, _dataStreamPositions)
            );

            DecompressByteRange(_presentStreamCompressedBuffer, _presentStreamDecompressedBuffer, _presentStream, _presentStreamPositions, ref _presentStreamDecompressedBufferLength);
            DecompressByteRange(_dataStreamCompressedBuffer, _dataStreamDecompressedBuffer, _dataStream, _dataStreamPositions, ref _dataStreamDecompressedBufferLength);
        }

        public override void Parse()
        {
            ReadBooleanStream(_presentStreamDecompressedBuffer, _presentStreamDecompressedBufferLength, _presentStreamPositions, _presentStreamValues, out var presentValuesRead);
            ReadNumericStream(_dataStreamDecompressedBuffer, _dataStreamDecompressedBufferLength, _dataStreamPositions, isSigned: true, _dataStreamValues, out var dataValuesRead);

            if (_presentStream != null)
            {
                var dataIndex = 0;
                for (int idx = 0; idx < presentValuesRead; idx++)
                {
                    if (_presentStreamValues[idx])
                        _values[_numValuesRead++] = _unixEpoch.AddTicks(_dataStreamValues[dataIndex++] * TimeSpan.TicksPerDay);
                    else
                        _values[_numValuesRead++] = null;
                }
            }
            else
            {
                for (int idx = 0; idx < dataValuesRead; idx++)
                    _values[_numValuesRead++] = _unixEpoch.AddTicks(_dataStreamValues[idx] * TimeSpan.TicksPerDay);
            }
        }
    }
}