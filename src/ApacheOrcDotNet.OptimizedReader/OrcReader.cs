using ApacheOrcDotNet.OptimizedReader.Buffers;
using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using ApacheOrcDotNet.Statistics;
using ProtoBuf;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader
{
    public class OrcReaderConfiguration
    {
        public int NumPreAllocatedDecompressionChunks { get; set; } = 3;
        public int OptimisticFileTailReadLength { get; set; } = 16 * 1024;
    }

    public sealed class OrcReader
    {
        private readonly OrcReaderConfiguration _configuration;
        private readonly IByteRangeProvider _byteRangeProvider;
        private readonly SpanFileTail _fileTail;
        private readonly Dictionary<int, List<StreamDetail>> _stripeStreams = new();
        private readonly ConcurrentDictionary<(int columnId, int stripeId), List<StreamDetail>> _columnStreams = new();
        private readonly ConcurrentDictionary<(int columnId, int stripeId), RowIndex> _rowGroupIndexes = new();
        private readonly Dictionary<string, (int Id, string Name, ColumnTypeKind Type)> _protoColumns = new();
        private readonly OrcFileProperties _orcFileProperties;

        public OrcReader(OrcReaderConfiguration configuration, IByteRangeProvider byteRangeProvider)
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
            }).ToDictionary(x => x.name.ToLower(), x => x);

            _orcFileProperties = new OrcFileProperties(
                _fileTail.PostScript.Compression,
                checked((int)_fileTail.PostScript.CompressionBlockSize),
                checked((int)_fileTail.Footer.RowIndexStride),
                _configuration.NumPreAllocatedDecompressionChunks
            );
        }

        public int NumValuesLoaded { get; set; }
        public int MaxValuesPerRowGroup => _orcFileProperties.MaxValuesToRead;

        public int GetNumberOfStripes() => _fileTail.Metadata.StripeStats.Count;

        public int GetNumberOfRowGroupEntries(int stripeId, int columnId) => GetColumnRowIndex(columnId, stripeId).Entry.Count;

        public OrcColumn GetColumn(string columnName)
        {
            if (!_protoColumns.TryGetValue(columnName?.ToLower(), out var column))
                throw new ArgumentException($"The column name '{columnName}' is invalid.");

            return new OrcColumn(column.Id, column.Name, column.Type);
        }

        public BaseColumnBuffer<byte[]> CreateBinaryColumnBuffer(OrcColumn column)
            => new BinaryColumnBuffer(_byteRangeProvider, _orcFileProperties, column);

        public BaseColumnBuffer<bool?> CreateBooleanColumnReader(OrcColumn column)
            => new BooleanColumnBuffer(_byteRangeProvider, _orcFileProperties, column);

        public BaseColumnBuffer<byte?> CreateByteColumnBuffer(OrcColumn column)
            => new ByteColumnBuffer(_byteRangeProvider, _orcFileProperties, column);

        public BaseColumnBuffer<DateTime?> CreateDateColumnBuffer(OrcColumn column)
            => new DateColumnBuffer(_byteRangeProvider, _orcFileProperties, column);

        public BaseColumnBuffer<double> CreateDecimalColumnBufferAsDouble(OrcColumn column)
            => new DecimalAsDoubleColumnBuffer(_byteRangeProvider, _orcFileProperties, column);

        public BaseColumnBuffer<decimal?> CreateDecimalColumnBuffer(OrcColumn column)
            => new DecimalColumnBuffer(_byteRangeProvider, _orcFileProperties, column);

        public BaseColumnBuffer<double?> CreateDoubleColumnBuffer(OrcColumn column)
            => new DoubleColumnBuffer(_byteRangeProvider, _orcFileProperties, column);

        public BaseColumnBuffer<double> CreateDoubleWithNullAsNaNColumnBuffer(OrcColumn column)
            => new DoubleWithNullAsNaNColumnBuffer(_byteRangeProvider, _orcFileProperties, column);

        public BaseColumnBuffer<float?> CreateFloatColumnBuffer(OrcColumn column)
            => new FloatColumnBuffer(_byteRangeProvider, _orcFileProperties, column);

        public BaseColumnBuffer<float> CreateFloatWithNullAsNaNColumnBuffer(OrcColumn column)
            => new FloatWithNullAsNaNColumnBuffer(_byteRangeProvider, _orcFileProperties, column);

        public BaseColumnBuffer<long?> CreateIntegerColumnBuffer(OrcColumn column)
            => new IntegerColumnBuffer(_byteRangeProvider, _orcFileProperties, column);

        public BaseColumnBuffer<string> CreateStringColumnBuffer(OrcColumn column)
            => new StringColumnBuffer(_byteRangeProvider, _orcFileProperties, column);

        public BaseColumnBuffer<DateTime?> CreateTimestampColumnBuffer(OrcColumn column)
            => new TimestampColumnBuffer(_byteRangeProvider, _orcFileProperties, column);

        public IEnumerable<int> FilterStripes(OrcColumn column, FilterValues filterValues)
            => FilterStripes(Enumerable.Range(0, _fileTail.Metadata.StripeStats.Count), column, filterValues);

        public IEnumerable<int> FilterStripes(IEnumerable<int> lookupStripeIds, OrcColumn column, FilterValues filterValues)
        {
            var columnStats = GetFileColumnStatistics(column.Id);

            if (!columnStats.InRange(column.Type, filterValues.Min, filterValues.Max))
                return Enumerable.Empty<int>();

            return lookupStripeIds.Where(stripeId =>
            {
                var stripeColumnStats = GetStripeColumnStatistics(column.Id, stripeId);
                return stripeColumnStats.InRange(column, filterValues.Min, filterValues.Max);
            }).ToList();
        }

        public IEnumerable<int> FilterRowGroups(int stripeId, OrcColumn column, FilterValues filterValues)
        {
            var rowIndex = GetColumnRowIndex(column.Id, stripeId);
            return FilterRowGroups(Enumerable.Range(0, rowIndex.Entry.Count), stripeId, column, filterValues);
        }

        public IEnumerable<int> FilterRowGroups(IEnumerable<int> lookupIndexes, int stripeId, OrcColumn column, FilterValues filterValues)
        {
            var rowIndex = GetColumnRowIndex(column.Id, stripeId);

            return lookupIndexes.Where(index =>
            {
                var rowIndexEntry = rowIndex.Entry[index];
                return rowIndexEntry.Statistics.InRange(column, filterValues.Min, filterValues.Max);
            }).ToList();
        }

        public async Task LoadDataAsync<TOutput>(int stripeId, int rowEntryIndexId, BaseColumnBuffer<TOutput> columnBuffer, bool discardPreviousData = true)
        {
            if (discardPreviousData)
                columnBuffer.Reset();

            var rowIndex = GetColumnRowIndex(columnBuffer.Column.Id, stripeId);
            var columnStreams = GetColumnDataStreams(stripeId, columnBuffer.Column, rowIndex, rowEntryIndexId);

            await columnBuffer.LoadDataAsync(stripeId, columnStreams);

            if (NumValuesLoaded == 0 || NumValuesLoaded > columnBuffer.Values.Length)
                NumValuesLoaded = columnBuffer.Values.Length;
        }

        public ColumnStatistics GetFileColumnStatistics(int columnId)
            => _fileTail.Footer.Statistics[columnId];

        public ColumnStatistics GetStripeColumnStatistics(int columnId, int stripeId)
            => _fileTail.Metadata.StripeStats[stripeId].ColStats[columnId];

        public RowIndex GetColumnRowIndex(int columnId, int stripeId)
        {
            var key = (columnId, stripeId);

            return _rowGroupIndexes.GetOrAdd(key, key =>
            {
                var streamDetails = GetStripeStreams(stripeId);
                var rowIndexStream = streamDetails.Where(s =>
                    s.StreamKind == StreamKind.RowIndex
                    && s.ColumnId == columnId
                ).Single();

                var compressedBuffer = new byte[rowIndexStream.Length];
                _byteRangeProvider.FillBuffer(compressedBuffer, rowIndexStream.FileOffset);

                var decompressedBuffer = CompressedData.CreateDecompressionBuffer(compressedBuffer, _fileTail.PostScript.Compression, checked((int)_fileTail.PostScript.CompressionBlockSize));
                var decompressedBufferLength = CompressedData.Decompress(compressedBuffer, decompressedBuffer, _fileTail.PostScript.Compression);

                return Serializer.Deserialize<RowIndex>(decompressedBuffer.AsSpan()[..decompressedBufferLength]);
            });
        }

        private SpanFileTail ReadFileTail()
        {
            int lengthToReadFromEnd = _configuration.OptimisticFileTailReadLength;
            while (true)
            {
                var fileTailBuffer = new byte[lengthToReadFromEnd];
                _byteRangeProvider.FillBufferFromEnd(fileTailBuffer);

                var success = SpanFileTail.TryRead(fileTailBuffer, out var fileTail, out var additionalBytesRequired);

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

                var compressedBuffer = new byte[stripeFooterLength];
                _byteRangeProvider.FillBuffer(compressedBuffer, stripeFooterStart);

                var decompressedBuffer = CompressedData.CreateDecompressionBuffer(compressedBuffer, _fileTail.PostScript.Compression, checked((int)_fileTail.PostScript.CompressionBlockSize));
                var decompressedBufferLength = CompressedData.Decompress(compressedBuffer, decompressedBuffer, _fileTail.PostScript.Compression);
                var streams = SpanStripeFooter.ReadStreamDetails(decompressedBuffer.AsSpan()[..decompressedBufferLength], (long)stripe.Offset);

                _stripeStreams.Add(stripeId, streams.ToList());
            }

            return _stripeStreams[stripeId];
        }

        private ColumnDataStreams GetColumnDataStreams(int stripeId, OrcColumn column, RowIndex rowIndex, int rowEntryIndex)
        {
            var key = (column.Id, stripeId);
            var rowIndexEntry = rowIndex.Entry[rowEntryIndex];
            var columnStreams = _columnStreams.GetOrAdd(key, key =>
            {
                var stripeStreams = GetStripeStreams(stripeId);
                return stripeStreams.Where(s =>
                    s.ColumnId == column.Id
                    && s.StreamKind != StreamKind.RowIndex
                ).ToList();
            });

            var result = new ColumnDataStreams();

            var present = columnStreams.SingleOrDefault(s => s.StreamKind == StreamKind.Present);
            if (present != null)
            {
                result.Present = present with
                {
                    Positions = GetPresentStreamPositions(present, rowIndexEntry),
                    Range = CalculatePresentRange(stripeId, present, rowIndex, rowEntryIndex)
                };
            }

            foreach (var stream in columnStreams)
            {
                if (stream.StreamKind == StreamKind.Present)
                    continue;

                result.EncodingKind = stream.EncodingKind;

                switch (stream.StreamKind)
                {
                    case StreamKind.Data:
                        result.Data = stream with
                        {
                            Positions = GetRequiredStreamPositions(present, stream, column, rowIndexEntry),
                            Range = CalculateDataRange(stripeId, present, stream, column, rowIndex, rowEntryIndex)
                        };
                        break;
                    case StreamKind.DictionaryData:
                        result.DictionaryData = stream with
                        {
                            Positions = GetRequiredStreamPositions(present, stream, column, rowIndexEntry),
                            Range = CalculateDataRange(stripeId, present, stream, column, rowIndex, rowEntryIndex)
                        };
                        break;
                    case StreamKind.Length:
                        result.Length = stream with
                        {
                            Positions = GetRequiredStreamPositions(present, stream, column, rowIndexEntry),
                            Range = CalculateDataRange(stripeId, present, stream, column, rowIndex, rowEntryIndex)
                        };
                        break;
                    case StreamKind.Secondary:
                        result.Secondary = stream with
                        {
                            Positions = GetRequiredStreamPositions(present, stream, column, rowIndexEntry),
                            Range = CalculateDataRange(stripeId, present, stream, column, rowIndex, rowEntryIndex)
                        };
                        break;
                    default:
                        throw new InvalidOperationException($"Stream kind {stream.StreamKind} is not supported.");
                }
            }

            return result;
        }

        private static StreamPositions GetPresentStreamPositions(StreamDetail presentStream, RowIndexEntry rowIndexEntry)
        {
            if (presentStream == null)
                return new();

            return new((int)rowIndexEntry.Positions[0], (int)rowIndexEntry.Positions[1], (int)rowIndexEntry.Positions[2], (int)rowIndexEntry.Positions[3]);
        }

        private static StreamPositions GetRequiredStreamPositions(StreamDetail presentStream, StreamDetail targetedStream, OrcColumn column, RowIndexEntry rowIndexEntry)
        {
            var positionStep = presentStream == null ? 0 : 4;

            ulong rowGroupOffset = (targetedStream.StreamKind, column.Type, targetedStream.EncodingKind) switch
            {
                (StreamKind.DictionaryData, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => rowIndexEntry.Positions[positionStep + 0],

                (StreamKind.Secondary, ColumnTypeKind.Timestamp, _) => rowIndexEntry.Positions[positionStep + 3],
                (StreamKind.Secondary, ColumnTypeKind.Decimal, _) => rowIndexEntry.Positions[positionStep + 2],

                (StreamKind.Length, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => rowIndexEntry.Positions[positionStep + 0],
                (StreamKind.Length, ColumnTypeKind.String, ColumnEncodingKind.DirectV2) => rowIndexEntry.Positions[positionStep + 2],
                (StreamKind.Length, ColumnTypeKind.Binary, ColumnEncodingKind.DirectV2) => rowIndexEntry.Positions[positionStep + 2],

                (StreamKind.Data, ColumnTypeKind.Timestamp, _) => rowIndexEntry.Positions[positionStep + 0],
                (StreamKind.Data, ColumnTypeKind.Decimal, _) => rowIndexEntry.Positions[positionStep + 0],
                (StreamKind.Data, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => rowIndexEntry.Positions[positionStep + 0],
                (StreamKind.Data, ColumnTypeKind.String, ColumnEncodingKind.DirectV2) => rowIndexEntry.Positions[positionStep + 0],
                (StreamKind.Data, ColumnTypeKind.Binary, ColumnEncodingKind.DirectV2) => rowIndexEntry.Positions[positionStep + 0],
                (StreamKind.Data, ColumnTypeKind.Short, _) => rowIndexEntry.Positions[positionStep + 0],
                (StreamKind.Data, ColumnTypeKind.Float, _) => rowIndexEntry.Positions[positionStep + 0],
                (StreamKind.Data, ColumnTypeKind.Double, _) => rowIndexEntry.Positions[positionStep + 0],
                (StreamKind.Data, ColumnTypeKind.Date, _) => rowIndexEntry.Positions[positionStep + 0],
                (StreamKind.Data, ColumnTypeKind.Long, _) => rowIndexEntry.Positions[positionStep + 0],
                (StreamKind.Data, ColumnTypeKind.Int, _) => rowIndexEntry.Positions[positionStep + 0],
                (StreamKind.Data, ColumnTypeKind.Byte, _) => rowIndexEntry.Positions[positionStep + 0],
                (StreamKind.Data, ColumnTypeKind.Boolean, _) => rowIndexEntry.Positions[positionStep + 0],

                _ => throw new NotImplementedException()
            };

            ulong rowEntryOffset = (targetedStream.StreamKind, column.Type, targetedStream.EncodingKind) switch
            {
                (StreamKind.DictionaryData, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => 0,

                (StreamKind.Secondary, ColumnTypeKind.Timestamp, _) => rowIndexEntry.Positions[positionStep + 4],
                (StreamKind.Secondary, ColumnTypeKind.Decimal, _) => rowIndexEntry.Positions[positionStep + 3],

                (StreamKind.Length, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => 0,
                (StreamKind.Length, ColumnTypeKind.String, ColumnEncodingKind.DirectV2) => rowIndexEntry.Positions[positionStep + 3],
                (StreamKind.Length, ColumnTypeKind.Binary, ColumnEncodingKind.DirectV2) => rowIndexEntry.Positions[positionStep + 3],

                (StreamKind.Data, ColumnTypeKind.Decimal, _) => rowIndexEntry.Positions[positionStep + 1],
                (StreamKind.Data, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => rowIndexEntry.Positions[positionStep + 1],
                (StreamKind.Data, ColumnTypeKind.String, ColumnEncodingKind.DirectV2) => rowIndexEntry.Positions[positionStep + 1],
                (StreamKind.Data, ColumnTypeKind.Binary, ColumnEncodingKind.DirectV2) => rowIndexEntry.Positions[positionStep + 1],
                (StreamKind.Data, ColumnTypeKind.Timestamp, _) => rowIndexEntry.Positions[positionStep + 1],
                (StreamKind.Data, ColumnTypeKind.Short, _) => rowIndexEntry.Positions[positionStep + 1],
                (StreamKind.Data, ColumnTypeKind.Double, _) => rowIndexEntry.Positions[positionStep + 1],
                (StreamKind.Data, ColumnTypeKind.Float, _) => rowIndexEntry.Positions[positionStep + 1],
                (StreamKind.Data, ColumnTypeKind.Date, _) => rowIndexEntry.Positions[positionStep + 1],
                (StreamKind.Data, ColumnTypeKind.Long, _) => rowIndexEntry.Positions[positionStep + 1],
                (StreamKind.Data, ColumnTypeKind.Int, _) => rowIndexEntry.Positions[positionStep + 1],
                (StreamKind.Data, ColumnTypeKind.Byte, _) => rowIndexEntry.Positions[positionStep + 1],
                (StreamKind.Data, ColumnTypeKind.Boolean, _) => rowIndexEntry.Positions[positionStep + 1],

                _ => throw new NotImplementedException()
            };

            ulong valuesToSkip = (targetedStream.StreamKind, column.Type, targetedStream.EncodingKind) switch
            {
                (StreamKind.DictionaryData, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => 0,

                (StreamKind.Secondary, ColumnTypeKind.Timestamp, _) => rowIndexEntry.Positions[positionStep + 5],
                (StreamKind.Secondary, ColumnTypeKind.Decimal, _) => rowIndexEntry.Positions[positionStep + 4],

                (StreamKind.Length, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => 0,
                (StreamKind.Length, ColumnTypeKind.String, ColumnEncodingKind.DirectV2) => rowIndexEntry.Positions[positionStep + 4],
                (StreamKind.Length, ColumnTypeKind.Binary, ColumnEncodingKind.DirectV2) => rowIndexEntry.Positions[positionStep + 4],

                (StreamKind.Data, ColumnTypeKind.Timestamp, _) => rowIndexEntry.Positions[positionStep + 2],
                (StreamKind.Data, ColumnTypeKind.Decimal, _) => 0,
                (StreamKind.Data, ColumnTypeKind.String, ColumnEncodingKind.DictionaryV2) => rowIndexEntry.Positions[positionStep + 2],
                (StreamKind.Data, ColumnTypeKind.String, ColumnEncodingKind.DirectV2) => 0,
                (StreamKind.Data, ColumnTypeKind.Binary, ColumnEncodingKind.DirectV2) => 0,
                (StreamKind.Data, ColumnTypeKind.Short, _) => rowIndexEntry.Positions[positionStep + 2],
                (StreamKind.Data, ColumnTypeKind.Double, _) => 0,
                (StreamKind.Data, ColumnTypeKind.Float, _) => 0,
                (StreamKind.Data, ColumnTypeKind.Date, _) => rowIndexEntry.Positions[positionStep + 2],
                (StreamKind.Data, ColumnTypeKind.Long, _) => rowIndexEntry.Positions[positionStep + 2],
                (StreamKind.Data, ColumnTypeKind.Int, _) => rowIndexEntry.Positions[positionStep + 2],
                (StreamKind.Data, ColumnTypeKind.Byte, _) => rowIndexEntry.Positions[positionStep + 2],
                (StreamKind.Data, ColumnTypeKind.Boolean, _) => rowIndexEntry.Positions[positionStep + 2],

                _ => throw new NotImplementedException()
            };

            ulong remainingBits = (targetedStream.StreamKind, column.Type, targetedStream.EncodingKind) switch
            {
                (StreamKind.Data, ColumnTypeKind.Boolean, _) => rowIndexEntry.Positions[positionStep + 3],
                _ => 0
            };

            return new((int)rowGroupOffset, (int)rowEntryOffset, (int)valuesToSkip, (int)remainingBits);
        }

        private static StreamRange CalculatePresentRange(int stripeId, StreamDetail presentStream, RowIndex rowIndex, int rowEntryIndex)
        {
            var rangeLength = 0;
            var currentEntry = rowIndex.Entry[rowEntryIndex];
            var currentPositions = GetPresentStreamPositions(presentStream, currentEntry);

            // Change in the current position marks the start of another entry.
            for (int idx = rowEntryIndex; idx < rowIndex.Entry.Count; idx++)
            {
                var nextOffset = GetPresentStreamPositions(presentStream, rowIndex.Entry[idx]);

                if (nextOffset.RowGroupOffset != currentPositions.RowGroupOffset)
                {
                    // Calculate the range length, adding bytes that may have been included into the next compressed chunk.
                    rangeLength = (nextOffset.RowGroupOffset - currentPositions.RowGroupOffset) + nextOffset.RowEntryOffset;
                    break;
                }
            }

            if (rangeLength == 0)
                rangeLength = presentStream.Length - currentPositions.RowGroupOffset;

            return new(stripeId, presentStream.FileOffset + currentPositions.RowGroupOffset, rangeLength);
        }

        private static StreamRange CalculateDataRange(int stripeId, StreamDetail presentStream, StreamDetail targetedStream, OrcColumn column, RowIndex rowIndex, int rowEntryIndex)
        {
            var rangeLength = 0;
            var currentEntry = rowIndex.Entry[rowEntryIndex];
            var currentPositions = GetRequiredStreamPositions(presentStream, targetedStream, column, currentEntry);

            // Change in the current position marks the start of another entry.
            for (int idx = rowEntryIndex; idx < rowIndex.Entry.Count; idx++)
            {
                var nextOffset = GetRequiredStreamPositions(presentStream, targetedStream, column, rowIndex.Entry[idx]);

                if (nextOffset.RowGroupOffset != currentPositions.RowGroupOffset)
                {
                    // Calculate the range length, adding bytes that may have been included into the next compressed chunk.
                    rangeLength = (nextOffset.RowGroupOffset - currentPositions.RowGroupOffset) + nextOffset.RowEntryOffset;
                    break;
                }
            }

            if (rangeLength == 0)
                rangeLength = targetedStream.Length - currentPositions.RowGroupOffset;

            return new(stripeId, targetedStream.FileOffset + currentPositions.RowGroupOffset, rangeLength);
        }
    }
}
