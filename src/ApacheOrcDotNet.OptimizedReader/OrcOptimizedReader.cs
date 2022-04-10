using ApacheOrcDotNet.OptimizedReader.ColumTypes.Specialized;
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

    public record ReaderContextOld(IEnumerable<StreamDetail> Streams, ColumnDetail Column, RowIndex Row, int RowEntryIndex)
    {
        public RowIndexEntry RowIndexEntry => Row.Entry[RowEntryIndex];

        public int GetRowEntryPosition(int positionIndex) => (int)RowIndexEntry.Positions[positionIndex];

        public StreamPositions GetPresentStreamPositions(StreamDetail presentStream)
        {
            if (presentStream == null)
                return new();

            return new(GetRowEntryPosition(0), GetRowEntryPosition(1), GetRowEntryPosition(2), GetRowEntryPosition(3));
        }

        public StreamPositions GetTargetedStreamPositions(StreamDetail presentStream, StreamDetail targetedStream)
        {
            var positionStep = presentStream == null ? 0 : 4;

            int rowGroupOffset = (targetedStream.StreamKind, Column.ColumnType, targetedStream.EncodingKind) switch
            {
                (StreamKind.DictionaryData, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => GetRowEntryPosition(positionStep + 0),

                (StreamKind.Secondary, ColumnTypeKind.Decimal, _) => GetRowEntryPosition(positionStep + 2),

                (StreamKind.Length, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => GetRowEntryPosition(positionStep + 0),
                (StreamKind.Length, ColumnTypeKind.String, ColumnEncodingKind.DirectV2) => GetRowEntryPosition(positionStep + 2),

                (StreamKind.Data, ColumnTypeKind.Decimal, _) => GetRowEntryPosition(positionStep + 0),
                (StreamKind.Data, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => GetRowEntryPosition(positionStep + 0),
                (StreamKind.Data, ColumnTypeKind.String, ColumnEncodingKind.DirectV2) => GetRowEntryPosition(positionStep + 0),
                (StreamKind.Data, ColumnTypeKind.Short, _) => GetRowEntryPosition(positionStep + 0),
                (StreamKind.Data, ColumnTypeKind.Long, _) => GetRowEntryPosition(positionStep + 0),
                (StreamKind.Data, ColumnTypeKind.Int, _) => GetRowEntryPosition(positionStep + 0),

                _ => throw new NotImplementedException()
            };
            int rowEntryOffset = (targetedStream.StreamKind, Column.ColumnType, targetedStream.EncodingKind) switch
            {
                (StreamKind.DictionaryData, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => 0,

                (StreamKind.Secondary, ColumnTypeKind.Decimal, _) => GetRowEntryPosition(positionStep + 3),

                (StreamKind.Length, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => 0,
                (StreamKind.Length, ColumnTypeKind.String, ColumnEncodingKind.DirectV2) => GetRowEntryPosition(positionStep + 3),

                (StreamKind.Data, ColumnTypeKind.Decimal, _) => GetRowEntryPosition(positionStep + 1),
                (StreamKind.Data, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => GetRowEntryPosition(positionStep + 1),
                (StreamKind.Data, ColumnTypeKind.String, ColumnEncodingKind.DirectV2) => GetRowEntryPosition(positionStep + 1),
                (StreamKind.Data, ColumnTypeKind.Short, _) => GetRowEntryPosition(positionStep + 1),
                (StreamKind.Data, ColumnTypeKind.Long, _) => GetRowEntryPosition(positionStep + 1),
                (StreamKind.Data, ColumnTypeKind.Int, _) => GetRowEntryPosition(positionStep + 1),

                _ => throw new NotImplementedException()
            };
            int valuesToSkip = (targetedStream.StreamKind, Column.ColumnType, targetedStream.EncodingKind) switch
            {
                (StreamKind.DictionaryData, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => 0,

                (StreamKind.Secondary, ColumnTypeKind.Decimal, _) => GetRowEntryPosition(positionStep + 4),

                (StreamKind.Length, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => 0,
                (StreamKind.Length, ColumnTypeKind.String, ColumnEncodingKind.DirectV2) => GetRowEntryPosition(positionStep + 4),

                (StreamKind.Data, ColumnTypeKind.Decimal, _) => 0,
                (StreamKind.Data, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => GetRowEntryPosition(positionStep + 2),
                (StreamKind.Data, ColumnTypeKind.String, ColumnEncodingKind.DirectV2) => 0,
                (StreamKind.Data, ColumnTypeKind.Short, _) => GetRowEntryPosition(positionStep + 2),
                (StreamKind.Data, ColumnTypeKind.Long, _) => GetRowEntryPosition(positionStep + 2),
                (StreamKind.Data, ColumnTypeKind.Int, _) => GetRowEntryPosition(positionStep + 2),

                _ => throw new NotImplementedException()
            };

            return new StreamPositions(rowGroupOffset, rowEntryOffset, valuesToSkip);
        }
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

        public BaseColumnReader<byte[]> CreateBinaryReader(int stripeId, int rowEntryIndex, string columnName)
        {
            var readerContext = GetReaderContext(stripeId, rowEntryIndex, columnName);
            return new ColumTypes.Specialized.BinaryReader(readerContext);
        }

        public BaseColumnReader<bool?> CreateBooleanReader(int stripeId, int rowEntryIndex, string columnName)
        {
            var readerContext = GetReaderContext(stripeId, rowEntryIndex, columnName);
            return new BooleanReader(readerContext);
        }

        public BaseColumnReader<byte?> CreateByteReader(int stripeId, int rowEntryIndex, string columnName)
        {
            var readerContext = GetReaderContext(stripeId, rowEntryIndex, columnName);
            return new ByteReader(readerContext);
        }

        public BaseColumnReader<DateTime?> CreateDateTimeReader(int stripeId, int rowEntryIndex, string columnName)
        {
            var readerContext = GetReaderContext(stripeId, rowEntryIndex, columnName);
            var columnEncoding = GetColumnEncodingKind(readerContext.Column.ColumnId, stripeId);

            return columnEncoding switch
            {
                ColumnEncodingKind.DirectV2 => new DateTimeReader(readerContext),
                _ => throw new InvalidOperationException()
            };
        }

        public BaseColumnReader<decimal?> CreateDecimalReader(int stripeId, int rowEntryIndex, string columnName)
        {
            var readerContext = GetReaderContext(stripeId, rowEntryIndex, columnName);
            var columnEncoding = GetColumnEncodingKind(readerContext.Column.ColumnId, stripeId);

            return columnEncoding switch
            {
                ColumnEncodingKind.DirectV2 => new DecimalDirectV2Reader(readerContext),
                _ => throw new InvalidOperationException()
            };
        }

        public BaseColumnReader<double> CreateDecimalAsDoubleReader(int stripeId, int rowEntryIndex, string columnName)
        {
            var readerContext = GetReaderContext(stripeId, rowEntryIndex, columnName);
            var columnEncoding = GetColumnEncodingKind(readerContext.Column.ColumnId, stripeId);

            return columnEncoding switch
            {
                ColumnEncodingKind.DirectV2 => new DecimalAsDoubleDirectV2Reader(readerContext),
                _ => throw new InvalidOperationException()
            };
        }

        public BaseColumnReader<double> CreateDoubleReader(int stripeId, int rowEntryIndex, string columnName)
        {
            var readerContext = GetReaderContext(stripeId, rowEntryIndex, columnName);
            var columnEncoding = GetColumnEncodingKind(readerContext.Column.ColumnId, stripeId);

            return columnEncoding switch
            {
                ColumnEncodingKind.DirectV2 => new DoubleReader(readerContext),
                _ => throw new InvalidOperationException()
            };
        }

        public BaseColumnReader<double> CreateFloatReader(int stripeId, int rowEntryIndex, string columnName)
        {
            var readerContext = GetReaderContext(stripeId, rowEntryIndex, columnName);
            var columnEncoding = GetColumnEncodingKind(readerContext.Column.ColumnId, stripeId);

            return columnEncoding switch
            {
                ColumnEncodingKind.DirectV2 => new DoubleReader(readerContext),
                _ => throw new InvalidOperationException()
            };
        }

        public BaseColumnReader<long?> CreateIntegerReader(int stripeId, int rowEntryIndex, string columnName)
        {
            var readerContext = GetReaderContext(stripeId, rowEntryIndex, columnName);
            return new IntegerDirectV2Reader(readerContext);
        }

        public BaseColumnReader<string> CreateStringReader(int stripeId, int rowEntryIndex, string columnName)
        {
            var readerContext = GetReaderContext(stripeId, rowEntryIndex, columnName);
            var columnEncoding = GetColumnEncodingKind(readerContext.Column.ColumnId, stripeId);

            return columnEncoding switch
            {
                ColumnEncodingKind.DictionaryV2 => new StringDictionaryV2Reader(readerContext),
                ColumnEncodingKind.DirectV2 => new StringDirectV2Reader(readerContext),
                _ => throw new InvalidOperationException()
            };
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

            return new ReaderContext(_byteRangeProvider, _fileTail, column, streams, rowGroup, rowEntryIndex);
        }

        private IEnumerable<StreamDetail> GetStripeStreams(int stripeId)
        {
            if (!_stripeStreams.ContainsKey(stripeId))
            {
                var stripe = _fileTail.Footer.Stripes[stripeId];
                var stripeFooterStart = (long)(stripe.Offset + stripe.IndexLength + stripe.DataLength);
                var stripeFooterLength = (int)stripe.FooterLength;

                var streams = _byteRangeProvider.DecompressAndParseByteRange(
                    stripeFooterStart,
                    stripeFooterLength,
                    _fileTail.PostScript.Compression,
                    (int)_fileTail.PostScript.CompressionBlockSize,
                    sequence => SpanStripeFooter.ReadStreamDetails(sequence, _columnDetails, (long)stripe.Offset)
                ).ToList();

                _stripeStreams.Add(stripeId, streams);
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

                var index = _byteRangeProvider.DecompressAndParseByteRange(
                    rowIndexStream.FileOffset,
                    rowIndexStream.Length,
                    _fileTail.PostScript.Compression,
                    (int)_fileTail.PostScript.CompressionBlockSize,
                    sequence => Serializer.Deserialize<RowIndex>(sequence)
                );

                _rowGroupIndexes.Add(key, index);
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
