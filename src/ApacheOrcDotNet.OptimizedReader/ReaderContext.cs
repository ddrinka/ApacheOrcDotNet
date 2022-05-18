using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;

namespace ApacheOrcDotNet.OptimizedReader
{
    public class ReaderContext
    {
        public ReaderContext(IByteRangeProvider byteRangeProvider, SpanFileTail fileTail, ColumnDetail columnDetails, IEnumerable<StreamDetail> columnStreams, RowIndex rowGroup, int rowEntryIndex)
        {
            ByteRangeProvider = byteRangeProvider ?? throw new ArgumentNullException(nameof(byteRangeProvider));
            FileTail = fileTail ?? throw new ArgumentNullException(nameof(fileTail));
            Streams = columnStreams ?? throw new ArgumentNullException(nameof(Stream));
            Column = columnDetails ?? throw new ArgumentNullException(nameof(columnDetails));
            Row = rowGroup ?? throw new ArgumentNullException(nameof(rowGroup));
            RowEntryIndex = rowEntryIndex;

            RowIndexEntry = Row.Entry[RowEntryIndex];
            CompressionKind = FileTail.PostScript.Compression;
            CompressionBlockSize = (int)FileTail.PostScript.CompressionBlockSize;
        }

        public IByteRangeProvider ByteRangeProvider { get; }
        public SpanFileTail FileTail { get; }
        public IEnumerable<StreamDetail> Streams { get; }
        public ColumnDetail Column { get; }
        public RowIndex Row { get; }
        public int RowEntryIndex { get; }
        public int StripeId { get; }

        public RowIndexEntry RowIndexEntry { get; }
        public CompressionKind CompressionKind { get; }
        public int CompressionBlockSize { get; }

        public ColumnEncodingKind ColumnEncodingKind { get; init; }
    }
}
