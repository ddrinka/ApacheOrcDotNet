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
		IList<long> CompressedLengths { get; }
		void CompleteAddingBlocks();
		void CopyDataBuffersTo(Stream outputStream);
		void CopyIndexBufferTo(Stream outputStream);
		void FillStripeFooter(Protocol.StripeFooter footer);
		void Reset();
	}
}
