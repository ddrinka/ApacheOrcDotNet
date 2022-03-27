﻿using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Protocol;
using ApacheOrcDotNet.Statistics;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader
{
    public class OrcReaderConfiguration
    {
        public int OptimisticFileTailReadLength { get; set; } = 16 * 1024;
    }

    public class ColumnDetail
    {
        public int ColumnId { get; set; }
        public string Name { get; set; }
        public ColumnTypeKind ColumnType { get; set; }
    }

    public class StripeDetail
    {
        public int StripeId { get; set; }
        public long RowCount { get; set; }
    }

    public sealed class OrcReader
    {
        readonly OrcReaderConfiguration _configuration;
        readonly IByteRangeProvider _byteRangeProvider;
        readonly SpanFileTail _fileTail;
        readonly Dictionary<int, List<StreamDetail>> _sliceStreams = new();

        public OrcReader(OrcReaderConfiguration configuration, IByteRangeProvider byteRangeProvider)
        {
            _configuration = configuration;
            _byteRangeProvider = byteRangeProvider;

            _fileTail = ReadFileTail();
            if (_fileTail.Footer.Types[0].Kind != ColumnTypeKind.Struct)
                throw new InvalidDataException($"The base type must be {nameof(ColumnTypeKind.Struct)}");

            ColumnDetails = _fileTail.Footer.Types[0].FieldNames
                .Select((name, i) => {
                    var subType = (int)_fileTail.Footer.Types[0].SubTypes[i];
                    var subTypeKind = _fileTail.Footer.Types[subType].Kind;
                    return new ColumnDetail { ColumnId = i, Name = name, ColumnType = subTypeKind };
                })
                .ToList();

            StripeDetails = _fileTail.Footer.Stripes
                .Select((stripe, i) => new StripeDetail { StripeId = i, RowCount = (long)stripe.NumberOfRows })
                .ToList();
        }
        
        public IEnumerable<ColumnDetail> ColumnDetails { get; }
        public IReadOnlyCollection<StripeDetail> StripeDetails { get; }

        public ColumnStatistics GetFileColumnStatistics(int columnId)
        {
            return _fileTail.Footer.Statistics[columnId];
        }

        public SpanRowGroupIndex ReadRowGroupIndex(int columnId, int stripeId)
        {
            if(!_sliceStreams.TryGetValue(stripeId, out var streamDetails))
            {
                streamDetails = ReadStripeFooter(stripeId).ToList();
                _sliceStreams.Add(stripeId, streamDetails);
            }

            var matchingStreamDetail = streamDetails.Single(s => s.ColumnId == columnId && s.StreamKind == StreamKind.RowIndex);

            var result = _byteRangeProvider.DecompressAndParseByteRange(
                matchingStreamDetail.FileOffset,
                matchingStreamDetail.Length,
                _fileTail.PostScript.Compression,
                (int)_fileTail.PostScript.CompressionBlockSize,
                sequence => new SpanRowGroupIndex(sequence)
            );

            return result;
        }

        IEnumerable<StreamDetail> ReadStripeFooter(int stripeId)
        {
            var stripe = _fileTail.Footer.Stripes[stripeId];
            var stripeFooterStart = (int)(stripe.Offset + stripe.IndexLength + stripe.DataLength); //TODO consider supporting >2TB files here
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
    }
}
