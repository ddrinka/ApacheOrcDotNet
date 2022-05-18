using ApacheOrcDotNet.OptimizedReader.ColumTypes;
using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using ApacheOrcDotNet.Statistics;
using ProtoBuf;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace ApacheOrcDotNet.OptimizedReader
{
    public class OrcOptimizedReaderConfiguration
    {
        public int OptimisticFileTailReadLength { get; set; } = 16 * 1024;
    }

    public sealed class OrcOptimizedReader
    {
        private readonly OrcOptimizedReaderConfiguration _configuration;
        private readonly IByteRangeProvider _byteRangeProvider;
        private readonly SpanFileTail _fileTail;
        private readonly List<ColumnDetail> _columnDetails = new();
        private readonly Dictionary<int, List<StreamDetail>> _stripeStreams = new();
        private readonly Dictionary<(int columnId, int stripeId), List<StreamDetail>> _columnStreams = new();
        private readonly Dictionary<(int columnId, int stripeId), ColumnEncodingKind> _columnEncodings = new();
        private readonly Dictionary<(int columnId, int stripeId), RowIndex> _rowGroupIndexes = new();

        public OrcOptimizedReader(OrcOptimizedReaderConfiguration configuration, IByteRangeProvider byteRangeProvider)
        {
            _configuration = configuration;
            _byteRangeProvider = byteRangeProvider;

            _fileTail = ReadFileTail();
            if (_fileTail.Footer.Types[0].Kind != ColumnTypeKind.Struct)
                throw new InvalidDataException($"The base type must be {nameof(ColumnTypeKind.Struct)}");

            _columnDetails = _fileTail.Footer.Types[0].FieldNames.Select((name, i) =>
            {
                var subType = (int)_fileTail.Footer.Types[0].SubTypes[i];
                var subTypeKind = _fileTail.Footer.Types[subType].Kind;
                return new ColumnDetail(ColumnId: subType, Name: name, ColumnType: subTypeKind);
            }).ToList();
        }

        public IEnumerable<int> GetStripeIds(string columnName, string min, string max)
            => GetStripeIds(Enumerable.Range(0, _fileTail.Metadata.StripeStats.Count), columnName, min, max);

        public IEnumerable<int> GetStripeIds(IEnumerable<int> lookupStripeIds, string columnName, string min, string max)
        {
            var column = GetColumnDetails(columnName);
            var columnStats = GetFileColumnStatistics(column.ColumnId);

            if (!columnStats.InRange(column.ColumnType, min, max))
                return Enumerable.Empty<int>();

            return lookupStripeIds.Where(stripeId =>
            {
                var stripeColumnStats = GetStripeColumnStatistics(column.ColumnId, stripeId);
                return stripeColumnStats.InRange(column.ColumnType, min, max);
            });
        }

        public IEnumerable<int> GetRowGroupIndexes(int stripeId, string columnName, string min, string max)
        {
            var column = GetColumnDetails(columnName);
            var rowIndex = GetRowGroupIndex(column.ColumnId, stripeId);
            return GetRowGroupIndexes(Enumerable.Range(0, rowIndex.Entry.Count), stripeId, columnName, min, max);
        }

        public IEnumerable<int> GetRowGroupIndexes(IEnumerable<int> lookupIndexes, int stripeId, string columnName, string min, string max)
        {
            var column = GetColumnDetails(columnName);
            var rowIndex = GetRowGroupIndex(column.ColumnId, stripeId);

            return lookupIndexes.Where(index =>
            {
                var rowIndexEntry = rowIndex.Entry[index];
                return rowIndexEntry.Statistics.InRange(column.ColumnType, min, max);
            });
        }

        public BaseColumnReader<byte[]> CreateBinaryColumnReader(int stripeId, int rowEntryIndex, string columnName)
        {
            var readerContext = GetReaderContext(stripeId, rowEntryIndex, columnName);
            return new OptimizedBinaryReader(readerContext);
        }

        public BaseColumnReader<bool?> CreateBooleanColumnReader(int stripeId, int rowEntryIndex, string columnName)
        {
            var readerContext = GetReaderContext(stripeId, rowEntryIndex, columnName);
            return new OptimizedBooleanReader(readerContext);
        }

        public BaseColumnReader<byte?> CreateByteColumnReader(int stripeId, int rowEntryIndex, string columnName)
        {
            var readerContext = GetReaderContext(stripeId, rowEntryIndex, columnName);
            return new OptimizedByteReader(readerContext);
        }

        public BaseColumnReader<DateTime?> CreateDateColumnReader(int stripeId, int rowEntryIndex, string columnName)
        {
            var readerContext = GetReaderContext(stripeId, rowEntryIndex, columnName);
            return new OptimizedDateReader(readerContext);
        }

        public BaseColumnReader<decimal?> CreateDecimalColumnReader(int stripeId, int rowEntryIndex, string columnName)
        {
            var readerContext = GetReaderContext(stripeId, rowEntryIndex, columnName);
            return new OptimizedDecimalReader(readerContext);
        }

        public BaseColumnReader<double> CreateDecimalColumnReaderAsDouble(int stripeId, int rowEntryIndex, string columnName)
        {
            var readerContext = GetReaderContext(stripeId, rowEntryIndex, columnName);
            return new OptimizedDecimalReader2(readerContext);
        }

        public BaseColumnReader<double> CreateDoubleColumnReader(int stripeId, int rowEntryIndex, string columnName)
        {
            var readerContext = GetReaderContext(stripeId, rowEntryIndex, columnName);
            return new OptimizedDoubleReader(readerContext);
        }

        public BaseColumnReader<double> CreateFloatColumnReader(int stripeId, int rowEntryIndex, string columnName)
        {
            var readerContext = GetReaderContext(stripeId, rowEntryIndex, columnName);
            return new OptimizedDoubleReader(readerContext);
        }

        public BaseColumnReader<long?> CreateIntegerColumnReader(int stripeId, int rowEntryIndex, string columnName)
        {
            var readerContext = GetReaderContext(stripeId, rowEntryIndex, columnName);
            return new OptimizedIntegerReader(readerContext);
        }

        public BaseColumnReader<string> CreateStringColumnReader(int stripeId, int rowEntryIndex, string columnName)
        {
            var readerContext = GetReaderContext(stripeId, rowEntryIndex, columnName);
            return new OptimizedStringReader(readerContext);
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

        private ReaderContext GetReaderContext(int stripeId, int rowEntryIndex, string columnName)
        {
            var column = GetColumnDetails(columnName);
            var streams = GetColumnStreams(column.ColumnId, stripeId);
            var rowGroup = GetRowGroupIndex(column.ColumnId, stripeId);

            return new ReaderContext(_byteRangeProvider, _fileTail, column, streams, rowGroup, rowEntryIndex)
            {
                ColumnEncodingKind = GetColumnEncodingKind(column.ColumnId, stripeId)
            };
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

                using (decompressedData)
                {
                    var streams = SpanStripeFooter.ReadStreamDetails(decompressedData.Sequence, _columnDetails, (long)stripe.Offset);
                    _stripeStreams.Add(stripeId, streams.ToList());
                }
            }

            return _stripeStreams[stripeId];
        }

        private ColumnDetail GetColumnDetails(string columnName)
            => _columnDetails.Single(colDetail => colDetail.Name.ToLower() == columnName.ToLower());

        private ColumnStatistics GetFileColumnStatistics(int columnId)
            => _fileTail.Footer.Statistics[columnId];

        private ColumnStatistics GetStripeColumnStatistics(int columnId, int stripeId)
            => _fileTail.Metadata.StripeStats[stripeId].ColStats[columnId];

        private RowIndex GetRowGroupIndex(int columnId, int stripeId)
        {
            var key = (columnId, stripeId);

            if (!_rowGroupIndexes.ContainsKey(key))
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

                using (decompressedData)
                {
                    var index = Serializer.Deserialize<RowIndex>(decompressedData.Sequence);
                    _rowGroupIndexes.Add(key, index);
                }
            }

            return _rowGroupIndexes[key];
        }

        private IEnumerable<StreamDetail> GetColumnStreams(int columnId, int stripeId)
        {
            var key = (columnId, stripeId);

            if (!_columnStreams.ContainsKey(key))
            {
                var stripeStreams = GetStripeStreams(stripeId);
                var columnStreams = stripeStreams.Where(s =>
                    s.ColumnId == columnId
                ).ToList();

                _columnStreams.Add(key, columnStreams);
            }

            return _columnStreams[key];
        }

        private ColumnEncodingKind GetColumnEncodingKind(int columnId, int stripeId)
        {
            var key = (columnId, stripeId);

            if (!_columnEncodings.ContainsKey(key))
            {
                var firstColumnStream = GetColumnStreams(columnId, stripeId).First();

                _columnEncodings.Add(key, firstColumnStream.EncodingKind);
            }

            return _columnEncodings[key];
        }
    }
}
