using ApacheOrcDotNet.Statistics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet
{
    public interface IStatistics
    {
		void AnnotatePosition(long compressedBufferOffset, long decompressedOffset, long rleValuesToConsume);
		void AnnotatePosition(long uncompressedOffset, long rleValuesToConsume);
		void FillColumnStatistics(ColumnStatistics columnStatistics);
		void FillPositionList(List<ulong> positions, Func<int,bool> bufferIndexMustBeIncluded);
	}
}
