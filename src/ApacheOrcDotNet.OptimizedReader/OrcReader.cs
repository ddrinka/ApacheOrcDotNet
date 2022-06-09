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
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader
{
    public class OrcReaderConfiguration
    {
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
        private readonly CompressionKind _compressionKind;
        private readonly int _compressionBlockSize;
        private readonly int _maxValuesToRead;

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
            }).ToDictionary(i => i.name.ToLower(), i => i);

            _compressionKind = _fileTail.PostScript.Compression;
            _compressionBlockSize = (int)_fileTail.PostScript.CompressionBlockSize;
            _maxValuesToRead = (int)_fileTail.Footer.RowIndexStride;
        }

        public int NumValues { get; set; }

        public OrcColumn GetColumn(int columnId, string min = null, string max = null)
        {
            if (columnId == 0 || columnId >= _protoColumns.Count)
                throw new InvalidOperationException($"The column Id '{columnId}' is invalid.");

            var columnPair = _protoColumns.ElementAt(columnId - 1);

            return GetColumn(columnPair.Value.Name, min, max);
        }

        public OrcColumn GetColumn(string columnName, string min = null, string max = null)
        {
            if (!_protoColumns.TryGetValue(columnName?.ToLower(), out var column))
                throw new InvalidOperationException($"The column name '{columnName}' is invalid.");

            var orcColumn = new OrcColumn(column.Id, column.Name, column.Type)
            {
                Min = min,
                Max = max
            };

            return orcColumn;
        }

        public BaseColumnBuffer<byte[]> CreateBinaryColumnBuffer(OrcColumn column)
        {
            var context = new OrcContext(_compressionKind, _compressionBlockSize, _maxValuesToRead);
            return new BinaryColumnBuffer(_byteRangeProvider, context, column);
        }

        public BaseColumnBuffer<bool?> CreateBooleanColumnReader(OrcColumn column)
        {
            var context = new OrcContext(_compressionKind, _compressionBlockSize, _maxValuesToRead);
            return new BooleanColumnBuffer(_byteRangeProvider, context, column);
        }

        public BaseColumnBuffer<byte?> CreateByteColumnBuffer(OrcColumn column)
        {
            var context = new OrcContext(_compressionKind, _compressionBlockSize, _maxValuesToRead);
            return new ByteColumnBuffer(_byteRangeProvider, context, column);
        }

        public BaseColumnBuffer<DateTime?> CreateDateColumnBuffer(OrcColumn column)
        {
            var context = new OrcContext(_compressionKind, _compressionBlockSize, _maxValuesToRead);
            return new DateColumnBuffer(_byteRangeProvider, context, column);
        }

        public BaseColumnBuffer<double> CreateDecimalColumnBufferAsDouble(OrcColumn column)
        {
            var context = new OrcContext(_compressionKind, _compressionBlockSize, _maxValuesToRead);
            return new DecimalAsDoubleColumnBuffer(_byteRangeProvider, context, column);
        }

        public BaseColumnBuffer<decimal?> CreateDecimalColumnBuffer(OrcColumn column)
        {
            var context = new OrcContext(_compressionKind, _compressionBlockSize, _maxValuesToRead);
            return new DecimalColumnBuffer(_byteRangeProvider, context, column);
        }

        public BaseColumnBuffer<double> CreateDoubleColumnBuffer(OrcColumn column)
        {
            var context = new OrcContext(_compressionKind, _compressionBlockSize, _maxValuesToRead);
            return new DoubleColumnBuffer(_byteRangeProvider, context, column);
        }

        public BaseColumnBuffer<float> CreateFloatColumnBuffer(OrcColumn column)
        {
            var context = new OrcContext(_compressionKind, _compressionBlockSize, _maxValuesToRead);
            return new FloatColumnBuffer(_byteRangeProvider, context, column);
        }

        public BaseColumnBuffer<long?> CreateIntegerColumnBuffer(OrcColumn column)
        {
            var context = new OrcContext(_compressionKind, _compressionBlockSize, _maxValuesToRead);
            return new IntegerColumnBuffer(_byteRangeProvider, context, column);
        }

        public BaseColumnBuffer<string> CreateStringColumnBuffer(OrcColumn column)
        {
            var context = new OrcContext(_compressionKind, _compressionBlockSize, _maxValuesToRead);
            return new StringColumnBuffer(_byteRangeProvider, context, column);
        }

        public BaseColumnBuffer<DateTime?> CreateTimestampColumnBuffer(OrcColumn column)
        {
            var context = new OrcContext(_compressionKind, _compressionBlockSize, _maxValuesToRead);
            return new TimestampColumnBuffer(_byteRangeProvider, context, column);
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
            var rowIndex = GetColumnRowIndex(column.Id, stripeId);
            return GetRowGroupIndexes(Enumerable.Range(0, rowIndex.Entry.Count), stripeId, column);
        }

        public IEnumerable<int> GetRowGroupIndexes(IEnumerable<int> lookupIndexes, int stripeId, OrcColumn column)
        {
            var rowIndex = GetColumnRowIndex(column.Id, stripeId);

            return lookupIndexes.Where(index =>
            {
                var rowIndexEntry = rowIndex.Entry[index];
                return rowIndexEntry.Statistics.InRange(column);
            });
        }

        public async Task LoadDataAsync<TOutput>(int stripeId, int rowEntryIndexId, BaseColumnBuffer<TOutput> columnBuffer)
        {
            var rowIndex = GetColumnRowIndex(columnBuffer.Column.Id, stripeId);
            var columnStreams = GetColumnDataStreams(stripeId, columnBuffer.Column, rowIndex, rowEntryIndexId);

            await columnBuffer.LoadDataAsync(stripeId, columnStreams);
        }

        public void Fill<TOutput>(BaseColumnBuffer<TOutput> columnBuffer, bool discardPreviousData = true)
        {
            if (discardPreviousData)
                columnBuffer.Reset();

            columnBuffer.Fill();

            if (NumValues == 0 || NumValues > columnBuffer.Values.Length)
                NumValues = columnBuffer.Values.Length;
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

                var decompressedData = _byteRangeProvider.DecompressByteRange(
                     rowIndexStream.FileOffset,
                     rowIndexStream.Length,
                     _fileTail.PostScript.Compression,
                     (int)_fileTail.PostScript.CompressionBlockSize
                 );

                return Serializer.Deserialize<RowIndex>(decompressedData.Sequence);
            });
        }

        private SpanFileTail ReadFileTail()
        {
            int lengthToReadFromEnd = _configuration.OptimisticFileTailReadLength;
            while (true)
            {
                var fileTailBufferRaw = ArrayPool<byte>.Shared.Rent(lengthToReadFromEnd);
                var fileTailBuffer = fileTailBufferRaw.AsSpan()[..lengthToReadFromEnd];

                _byteRangeProvider.GetRangeFromEnd(fileTailBuffer, lengthToReadFromEnd);

                var success = SpanFileTail.TryRead(fileTailBuffer, out var fileTail, out var additionalBytesRequired);
                ArrayPool<byte>.Shared.Return(fileTailBufferRaw);

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
                    Range = CalculatePresentRange(stripeId, present, column, rowIndex, rowEntryIndex)
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

        private StreamPositions GetPresentStreamPositions(StreamDetail presentStream, RowIndexEntry rowIndexEntry)
        {
            if (presentStream == null)
                return new();

            return new((int)rowIndexEntry.Positions[0], (int)rowIndexEntry.Positions[1], (int)rowIndexEntry.Positions[2], (int)rowIndexEntry.Positions[3]);
        }

        private StreamPositions GetRequiredStreamPositions(StreamDetail presentStream, StreamDetail targetedStream, OrcColumn column, RowIndexEntry rowIndexEntry)
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

        private StreamRange CalculatePresentRange(int stripeId, StreamDetail presentStream, OrcColumn column, RowIndex rowIndex, int rowEntryIndex)
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
                    // Calculate the range length, adding possible bytes that might have been included into the next compressed chunk.
                    rangeLength = (nextOffset.RowGroupOffset - currentPositions.RowGroupOffset) + nextOffset.RowEntryOffset;
                    break;
                }
            }

            if (rangeLength == 0)
                rangeLength = presentStream.Length - currentPositions.RowGroupOffset;

            return new(stripeId, presentStream.FileOffset + currentPositions.RowGroupOffset, rangeLength);
        }

        private StreamRange CalculateDataRange(int stripeId, StreamDetail presentStream, StreamDetail targetedStream, OrcColumn column, RowIndex rowIndex, int rowEntryIndex)
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
                    // Calculate the range length, adding possible bytes that might have been included into the next compressed chunk.
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