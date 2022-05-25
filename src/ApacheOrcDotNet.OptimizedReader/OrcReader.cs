using ApacheOrcDotNet.OptimizedReader.Buffers;
using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using ApacheOrcDotNet.Statistics;
using ProtoBuf;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace ApacheOrcDotNet.OptimizedReader
{
    public class OrcOptimizedReaderConfiguration
    {
        public int OptimisticFileTailReadLength { get; set; } = 16 * 1024;
    }

    public sealed class OrcReader
    {
        private readonly OrcOptimizedReaderConfiguration _configuration;
        private readonly IByteRangeProvider _byteRangeProvider;
        private readonly SpanFileTail _fileTail;
        private readonly Dictionary<int, List<StreamDetail>> _stripeStreams = new();
        private readonly ConcurrentDictionary<(int columnId, int stripeId), List<StreamDetail>> _columnStreams = new();
        private readonly ConcurrentDictionary<(int columnId, int stripeId), RowIndex> _rowGroupIndexes = new();
        private readonly Dictionary<string, (int Id, string Name, ColumnTypeKind Type)> _protoColumns = new();
        private readonly CompressionKind _compressionKind;
        private readonly int _compressionBlockSize;
        private readonly int _maxValuesToRead;
        private int _nextColumnIndex = 0;

        public OrcReader(OrcOptimizedReaderConfiguration configuration, IByteRangeProvider byteRangeProvider)
        {
            _configuration = configuration;
            _byteRangeProvider = byteRangeProvider;

            _fileTail = ReadFileTail();
            if (_fileTail.Footer.Types[0].Kind != ColumnTypeKind.Struct)
                throw new InvalidDataException($"The base type must be {nameof(ColumnTypeKind.Struct)}");

            _protoColumns = _fileTail.Footer.Types[0].FieldNames.Select((name, i) =>
            {
                var subType = (int)_fileTail.Footer.Types[0].SubTypes[i];
                var subTypeKind = _fileTail.Footer.Types[subType].Kind;
                return (subType, name, subTypeKind);
            }).ToDictionary(i => i.name.ToLower(), i => i);

            _compressionKind = _fileTail.PostScript.Compression;
            _compressionBlockSize = (int)_fileTail.PostScript.CompressionBlockSize;
            _maxValuesToRead = (int)_fileTail.Footer.RowIndexStride;
        }

        public OrcColumn CreateColumn(string columnName, string min = null, string max = null)
        {
            if (!_protoColumns.TryGetValue(columnName?.ToLower(), out var column))
                throw new InvalidOperationException($"The column '{columnName}' is invalid.");

            var orcColumn = new OrcColumn(column.Id, _nextColumnIndex++, column.Name, column.Type)
            {
                Min = min,
                Max = max
            };

            return orcColumn;
        }

        public BaseColumnBuffer<byte[]> CreateBinaryColumnBuffer(OrcColumn column)
        {
            var context = new OrcContextNew(_compressionKind, _compressionBlockSize, _maxValuesToRead);
            return new BinaryColumnBuffer(_byteRangeProvider, context, column);
        }

        public BaseColumnBuffer<bool?> CreateBooleanColumnReader(OrcColumn column)
        {
            var context = new OrcContextNew(_compressionKind, _compressionBlockSize, _maxValuesToRead);
            return new BooleanColumnBuffer(_byteRangeProvider, context, column);
        }

        public BaseColumnBuffer<byte?> CreateByteColumnBuffer(OrcColumn column)
        {
            var context = new OrcContextNew(_compressionKind, _compressionBlockSize, _maxValuesToRead);
            return new ByteColumnBuffer(_byteRangeProvider, context, column);
        }

        public BaseColumnBuffer<DateTime?> CreateDateColumnBuffer(OrcColumn column)
        {
            var context = new OrcContextNew(_compressionKind, _compressionBlockSize, _maxValuesToRead);
            return new DateColumnBuffer(_byteRangeProvider, context, column);
        }

        public BaseColumnBuffer<decimal?> CreateDecimalColumnReader(OrcColumn column)
        {
            var context = new OrcContextNew(_compressionKind, _compressionBlockSize, _maxValuesToRead);
            return new DecimalColumnBuffer(_byteRangeProvider, context, column);
        }

        public BaseColumnBuffer<double> CreateDecimalColumnBufferAsDouble(OrcColumn column)
        {
            var context = new OrcContextNew(_compressionKind, _compressionBlockSize, _maxValuesToRead);
            return new DecimalAsDoubleColumnBuffer(_byteRangeProvider, context, column);
        }

        public BaseColumnBuffer<double> CreateDoubleColumnBuffer(OrcColumn column)
        {
            var context = new OrcContextNew(_compressionKind, _compressionBlockSize, _maxValuesToRead);
            return new DoubleColumnBuffer(_byteRangeProvider, context, column);
        }

        public BaseColumnBuffer<float> CreateFloatColumnBuffer(OrcColumn column)
        {
            var context = new OrcContextNew(_compressionKind, _compressionBlockSize, _maxValuesToRead);
            return new FloatColumnBuffer(_byteRangeProvider, context, column);
        }

        public BaseColumnBuffer<long?> CreateIntegerColumnBuffer(OrcColumn column)
        {
            var context = new OrcContextNew(_compressionKind, _compressionBlockSize, _maxValuesToRead);
            return new IntegerColumnBuffer(_byteRangeProvider, context, column);
        }

        public BaseColumnBuffer<string> CreateStringColumnBuffer(OrcColumn column)
        {
            var context = new OrcContextNew(_compressionKind, _compressionBlockSize, _maxValuesToRead);
            return new StringColumnBuffer(_byteRangeProvider, context, column);
        }

        public BaseColumnBuffer<DateTime?> CreateTimestampColumnBuffer(OrcColumn column)
        {
            var context = new OrcContextNew(_compressionKind, _compressionBlockSize, _maxValuesToRead);
            return new TimestampColumnBuffer(_byteRangeProvider, context, column);
        }

        public void FillBuffer<TOutput>(int stripeId, int rowEntryIndexId, BaseColumnBuffer<TOutput> columnBuffer)
        {
            var columnStreams = GetColumnStreams(columnBuffer.Column.Id, stripeId);
            var rowIndexEntry = GetRowGroupIndex(columnBuffer.Column.Id, stripeId).Entry[rowEntryIndexId];

            columnBuffer.Fill(stripeId, columnStreams, rowIndexEntry);
        }

        public IEnumerable<int> GetStripeIds(OrcColumn column)
            => GetStripeIds(Enumerable.Range(0, _fileTail.Metadata.StripeStats.Count), column);

        public IEnumerable<int> GetStripeIds(IEnumerable<int> lookupStripeIds, OrcColumn column)
        {
            var columnStats = GetFileColumnStatistics(column.Id);

            if (!columnStats.InRange(column.Type, column.Min, column.Max))
                return Enumerable.Empty<int>();

            return lookupStripeIds.Where(stripeId =>
            {
                var stripeColumnStats = GetStripeColumnStatistics(column.Id, stripeId);
                return stripeColumnStats.InRange(column);
            });
        }

        public IEnumerable<int> GetRowGroupIndexes(int stripeId, OrcColumn column)
        {
            var rowIndex = GetRowGroupIndex(column.Id, stripeId);
            return GetRowGroupIndexes(Enumerable.Range(0, rowIndex.Entry.Count), stripeId, column);
        }

        public IEnumerable<int> GetRowGroupIndexes(IEnumerable<int> lookupIndexes, int stripeId, OrcColumn column)
        {
            var rowIndex = GetRowGroupIndex(column.Id, stripeId);

            return lookupIndexes.Where(index =>
            {
                var rowIndexEntry = rowIndex.Entry[index];
                return rowIndexEntry.Statistics.InRange(column);
            });
        }

        private SpanFileTail ReadFileTail()
        {
            int lengthToReadFromEnd = _configuration.OptimisticFileTailReadLength;
            while (true)
            {
                var buffer = ArrayPool<byte>.Shared.Rent(lengthToReadFromEnd);
                var bufferSpan = buffer.AsSpan()[..lengthToReadFromEnd];
                _byteRangeProvider.GetRangeFromEnd(bufferSpan, lengthToReadFromEnd);
                var success = SpanFileTail.TryRead(bufferSpan, out var fileTail, out var additionalBytesRequired);
                ArrayPool<byte>.Shared.Return(buffer);

                if (success)
                    return fileTail;

                lengthToReadFromEnd += additionalBytesRequired;
            }
        }

        private IEnumerable<StreamDetail> GetStripeStreams(int stripeId)
        {
            if (!_stripeStreams.ContainsKey(stripeId))
            {
                var stripe = _fileTail.Footer.Stripes[stripeId];
                var stripeFooterStart = (long)(stripe.Offset + stripe.IndexLength + stripe.DataLength);
                var stripeFooterLength = (int)stripe.FooterLength;

                var decompressedData = _byteRangeProvider.DecompressByteRange(
                    stripeFooterStart,
                    stripeFooterLength,
                    _fileTail.PostScript.Compression,
                    (int)_fileTail.PostScript.CompressionBlockSize
                );

                var streams = SpanStripeFooter.ReadStreamDetails(decompressedData.Sequence, (long)stripe.Offset);

                _stripeStreams.Add(stripeId, streams.ToList());
            }

            return _stripeStreams[stripeId];
        }

        private ColumnStatistics GetFileColumnStatistics(int columnId)
            => _fileTail.Footer.Statistics[columnId];

        private ColumnStatistics GetStripeColumnStatistics(int columnId, int stripeId)
            => _fileTail.Metadata.StripeStats[stripeId].ColStats[columnId];

        private RowIndex GetRowGroupIndex(int columnId, int stripeId)
        {
            var key = (columnId, stripeId);

            return _rowGroupIndexes.GetOrAdd(key, key =>
            {
                var streamDetails = GetStripeStreams(stripeId);
                var rowIndexStream = streamDetails.Where(s =>
                    s.StreamKind == StreamKind.RowIndex
                    && s.ColumnId == columnId
                ).Single();

                var decompressedData = _byteRangeProvider.DecompressByteRange(
                     rowIndexStream.FileOffset,
                     rowIndexStream.Length,
                     _fileTail.PostScript.Compression,
                     (int)_fileTail.PostScript.CompressionBlockSize
                 );

                return Serializer.Deserialize<RowIndex>(decompressedData.Sequence);
            });
        }

        private IEnumerable<StreamDetail> GetColumnStreams(int columnId, int stripeId)
        {
            var key = (columnId, stripeId);

            return _columnStreams.GetOrAdd(key, key =>
            {
                var stripeStreams = GetStripeStreams(stripeId);
                return stripeStreams.Where(s =>
                    s.ColumnId == columnId
                ).ToList();
            });
        }
    }
}