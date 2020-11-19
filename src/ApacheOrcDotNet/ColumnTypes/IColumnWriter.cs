using ApacheOrcDotNet.Compression;
using System.Collections.Generic;

namespace ApacheOrcDotNet.ColumnTypes {
    public interface IColumnWriter {
        List<IStatistics> Statistics { get; }
        Protocol.ColumnEncodingKind ColumnEncoding { get; }
        OrcCompressedBuffer[] Buffers { get; }
        long CompressedLength { get; }
        uint ColumnId { get; }
        void FlushBuffers();
        void Reset();
    }
}
