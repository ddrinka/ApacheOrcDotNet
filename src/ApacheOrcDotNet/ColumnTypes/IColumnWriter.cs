using ApacheOrcDotNet.Compression;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
    public interface IColumnWriter
    {
		List<IStatistics> Statistics { get; }
		Protocol.ColumnEncodingKind ColumnEncoding { get; }
		OrcCompressedBuffer[] Buffers { get; }
		long CompressedLength { get; }
		uint ColumnId { get; }
		void FlushBuffers();
		void Reset();
	}
}
