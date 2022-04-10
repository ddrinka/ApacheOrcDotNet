using ApacheOrcDotNet.Protocol;
using ApacheOrcDotNet.Statistics;
using ApacheOrcDotNet.Stripes;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace ApacheOrcDotNet.OptimizedReader
{
    public record FilterArg(string ColumnName, string MinValue, string MaxValue);
    public record Filter(int ColumnId, ColumnTypeKind ColumnType, string MinValue, string MaxValue);
    public record ColumnDetail(int ColumnId, string Name, ColumnTypeKind ColumnType);
    public record StripeDetail(int StripeId, long RowCount);
    public record FilterCriteria(string ColumnName, string minValue, string maxValue);

    public sealed class OrcReader
    {
        private readonly OrcOptimizedReaderConfiguration _configuration;
        private readonly IByteRangeProvider _byteRangeProvider;
        private readonly SpanFileTail _fileTail;
        private readonly Dictionary<int, List<StreamDetail>> _stripeStreams = new();

        public OrcReader(OrcOptimizedReaderConfiguration configuration, IByteRangeProvider byteRangeProvider)
        {
            _configuration = configuration;
            _byteRangeProvider = byteRangeProvider;

            _fileTail = ReadFileTail();
            if (_fileTail.Footer.Types[0].Kind != ColumnTypeKind.Struct)
                throw new InvalidDataException($"The base type must be {nameof(ColumnTypeKind.Struct)}");

            ColumnDetails = _fileTail.Footer.Types[0].FieldNames
                .Select((name, i) =>
                {
                    var subType = (int)_fileTail.Footer.Types[0].SubTypes[i];
                    var subTypeKind = _fileTail.Footer.Types[subType].Kind;
                    return new ColumnDetail(ColumnId: subType, Name: name, ColumnType: subTypeKind);
                })
                .ToList();

            StripeDetails = _fileTail.Footer.Stripes
                .Select((stripe, i) => new StripeDetail(StripeId: i, RowCount: (long)stripe.NumberOfRows))
                .ToList();
        }

        public int GetColumnId(string columnName) => ColumnDetails.SingleOrDefault(colDetail =>
            colDetail.Name.ToLower() == columnName.ToLower()
        ).ColumnId;

        IEnumerable<StreamDetail> GetStripeStreams(int stripeId)
        {
            if (!_stripeStreams.ContainsKey(stripeId))
            {
                var stripe = _fileTail.Footer.Stripes[stripeId];
                var stripeFooterStart = (int)(stripe.Offset + stripe.IndexLength + stripe.DataLength); //TODO consider supporting >2TB files here
                var stripeFooterLength = (int)stripe.FooterLength;

                var streams = _byteRangeProvider.DecompressAndParseByteRange(
                    stripeFooterStart,
                    stripeFooterLength,
                    _fileTail.PostScript.Compression,
                    (int)_fileTail.PostScript.CompressionBlockSize,
                    sequence => SpanStripeFooter.ReadStreamDetails(sequence, ColumnDetails, (long)stripe.Offset)
                ).ToList();

                _stripeStreams.Add(stripeId, streams);
            }

            return _stripeStreams[stripeId];
        }

        IEnumerable<StreamDetail> ReadStripeFooter(int stripeId)
        {
            var stripe = _fileTail.Footer.Stripes[stripeId];
            var stripeFooterStart = (int)(stripe.Offset + stripe.IndexLength + stripe.DataLength);
            var stripeFooterLength = (int)stripe.FooterLength;

            var result = _byteRangeProvider.DecompressAndParseByteRange(
                stripeFooterStart,
                stripeFooterLength,
                _fileTail.PostScript.Compression,
                (int)_fileTail.PostScript.CompressionBlockSize,
                sequence => SpanStripeFooter.ReadStreamDetails(sequence, ColumnDetails, (long)stripe.Offset)
            );

            return result;
        }

        SpanFileTail ReadFileTail()
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









        #region Old Implementation / Tests

        public IEnumerable<ColumnDetail> ColumnDetails { get; }
        public IReadOnlyCollection<StripeDetail> StripeDetails { get; }

        public List<string> ReadOldSource(int stripeId)
        {
            var fileStream = new FileStream(@"F:\integritas\ergon\_data\_orc_db_files\2022.03.18.tmp.orc", FileMode.Open);
            var stripeReaderCollection = new StripeReaderCollection(fileStream, _fileTail.Footer, _fileTail.PostScript.Compression);
            var stripeStreamReaderCollection = stripeReaderCollection[stripeId].GetStripeStreamCollection();

            var valuesReader = new ColumnTypes.StringReader(stripeStreamReaderCollection, 1);

            var values = valuesReader.Read().ToList();

            fileStream.Dispose();

            return values;
        }

        public List<string> ReadOldSymbol(int stripeId)
        {
            var fileStream = new FileStream(@"F:\integritas\ergon\_data\_orc_db_files\2022.03.18.tmp.orc", FileMode.Open);
            var stripeReaderCollection = new StripeReaderCollection(fileStream, _fileTail.Footer, _fileTail.PostScript.Compression);
            var stripeStreamReaderCollection = stripeReaderCollection[stripeId].GetStripeStreamCollection();

            var valuesReader = new ColumnTypes.StringReader(stripeStreamReaderCollection, 8);

            var values = valuesReader.Read().ToList();

            fileStream.Dispose();

            return values;
        }

        public List<decimal?> ReadOldTime(int stripeId)
        {
            var fileStream = new FileStream(@"F:\integritas\ergon\_data\_orc_db_files\2022.03.18.tmp.orc", FileMode.Open);
            var stripeReaderCollection = new StripeReaderCollection(fileStream, _fileTail.Footer, _fileTail.PostScript.Compression);
            var stripeStreamReaderCollection = stripeReaderCollection[stripeId].GetStripeStreamCollection();

            var valuesReader = new ColumnTypes.DecimalReader(stripeStreamReaderCollection, 5);

            var values = valuesReader.Read().ToList();

            fileStream.Dispose();

            return values;
        }

        public List<long?> ReadOldSize(int stripeId)
        {
            var fileStream = new FileStream(@"F:\integritas\ergon\_data\_orc_db_files\2022.03.18.tmp.orc", FileMode.Open);
            var stripeReaderCollection = new StripeReaderCollection(fileStream, _fileTail.Footer, _fileTail.PostScript.Compression);
            var stripeStreamReaderCollection = stripeReaderCollection[stripeId].GetStripeStreamCollection();

            var valuesReader = new ColumnTypes.LongReader(stripeStreamReaderCollection, 10);

            var values = valuesReader.Read().ToList();

            fileStream.Dispose();

            return values;
        }

        private IEnumerable<int> GetFilteredStripeIds(List<Filter> filters)
        {
            var filteredStripeIds = new List<int>();

            for (int stripeIndex = 0; stripeIndex < _fileTail.Metadata.StripeStats.Count; stripeIndex++)
            {
                var isMatch = true;

                for (int colIndex = 0; colIndex < filters.Count; colIndex++)
                {
                    var filter = filters[colIndex];
                    var lookupStripeCol = GetStripeColumnStatistics(filter.ColumnId, stripeIndex);

                    if (!lookupStripeCol.InRange(filter.ColumnType, filter.MinValue, filter.MaxValue))
                    {
                        isMatch = false;
                        break;
                    }
                }

                if (isMatch)
                    filteredStripeIds.Add(stripeIndex);
            }

            return filteredStripeIds;
        }

        public ColumnStatistics GetFileColumnStatistics(int columnId)
        {
            return _fileTail.Footer.Statistics[columnId];
        }

        public ColumnStatistics GetStripeColumnStatistics(int columnId, int stripeId)
        {
            return _fileTail.Metadata.StripeStats[stripeId].ColStats[columnId];
        }

        public RowIndex GetRowGroupIndex(int columnId, int stripeId)
        {
            var streamDetails = GetStripeStreams(stripeId);
            var rowIndexStream = streamDetails.Where(s =>
                s.StreamKind == StreamKind.RowIndex
                && s.ColumnId == columnId
            ).Single();

            return _byteRangeProvider.DecompressAndParseByteRange(
                rowIndexStream.FileOffset,
                rowIndexStream.Length,
                _fileTail.PostScript.Compression,
                (int)_fileTail.PostScript.CompressionBlockSize,
                sequence => SpanRowGroupIndex.ReadRowGroupIndex(sequence)
            );
        }

        public IEnumerable<RowGroupDetail> ReadRowGroupIndex(int columnId, int stripeId)
        {
            var streamDetails = GetStripeStreams(stripeId);
            var streamsForColumn = streamDetails.Where(s => s.ColumnId == columnId).ToList();
            var rowIndexStream = streamsForColumn.First(s => s.StreamKind == StreamKind.RowIndex);

            var result = _byteRangeProvider.DecompressAndParseByteRange(
                rowIndexStream.FileOffset,
                rowIndexStream.Length,
                _fileTail.PostScript.Compression,
                (int)_fileTail.PostScript.CompressionBlockSize,
                sequence => SpanRowGroupIndex.ReadRowGroupDetails(sequence, streamsForColumn, _fileTail.PostScript.Compression)
            );

            return result;
        }

        #endregion
    }
}
