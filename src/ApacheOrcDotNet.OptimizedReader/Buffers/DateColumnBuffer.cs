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
        private readonly bool[] _presentStreamBuffer;
        private readonly long[] _dataStreamBuffer;
        private byte[] _presentInputBuffer;
        private byte[] _presentOutputBuffer;
        private byte[] _dataInputBuffer;
        private byte[] _dataOutputBuffer;

        public DateColumnBuffer(IByteRangeProvider byteRangeProvider, OrcContext context, OrcColumn column) : base(byteRangeProvider, context, column)
        {
            _presentStreamBuffer = new bool[_context.MaxValuesToRead];
            _dataStreamBuffer = new long[_context.MaxValuesToRead];

            _presentInputBuffer = _arrayPool.Rent(_context.MaxCompressedBufferLength);
            _presentOutputBuffer = _arrayPool.Rent(_context.MaxDecompresseBufferLength);

            _dataInputBuffer = _arrayPool.Rent(_context.MaxCompressedBufferLength);
            _dataOutputBuffer = _arrayPool.Rent(_context.MaxDecompresseBufferLength);
        }

        public override void Fill(int stripeId, IEnumerable<StreamDetail> columnStreams, RowIndexEntry rowIndexEntry)
        {
            // Streams
            var presentStream = GetColumnStream(columnStreams, StreamKind.Present, isRequired: false);
            var dataStream = GetColumnStream(columnStreams, StreamKind.Data);

            // Stream Positions
            var presentPositions = GetPresentStreamPositions(presentStream, rowIndexEntry);
            var dataPositions = GetTargetDataStreamPositions(presentStream, dataStream, rowIndexEntry);

            // Stream Byte Ranges
            (int present, int data) rangeSizes = default;
            Parallel.Invoke(
                () => GetByteRange(_presentInputBuffer, presentStream, presentPositions, ref rangeSizes.present),
                () => GetByteRange(_dataInputBuffer, dataStream, dataPositions, ref rangeSizes.data)
            );

            // Decompress Byte Ranges
            (int present, int data) decompressedSizes = default;
            DecompressByteRange(_presentInputBuffer, _presentOutputBuffer, presentStream, presentPositions, ref decompressedSizes.present);
            DecompressByteRange(_dataInputBuffer, _dataOutputBuffer, dataStream, dataPositions, ref decompressedSizes.data);

            // Parse Decompressed Bytes
            (int present, int data) valuesRead = default;
            ReadBooleanStream(_presentOutputBuffer, decompressedSizes.present, presentPositions, _presentStreamBuffer, ref valuesRead.present);
            ReadNumericStream(_dataOutputBuffer, decompressedSizes.data, dataPositions, isSigned: true, _dataStreamBuffer, ref valuesRead.data);

            if (presentStream != null)
            {
                var dataIndex = 0;
                for (int idx = 0; idx < valuesRead.present; idx++)
                {
                    if (_presentStreamBuffer[idx])
                        _values[_numValuesRead++] = _unixEpoch.AddTicks(_dataStreamBuffer[dataIndex++] * TimeSpan.TicksPerDay);
                    else
                        _values[_numValuesRead++] = null;
                }
            }
            else
            {
                for (int idx = 0; idx < valuesRead.data; idx++)
                    _values[_numValuesRead++] = _unixEpoch.AddTicks(_dataStreamBuffer[idx] * TimeSpan.TicksPerDay);
            }
        }
    }
}