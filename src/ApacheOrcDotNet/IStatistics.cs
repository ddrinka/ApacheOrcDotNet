using ApacheOrcDotNet.Statistics;
using System;
using System.Collections.Generic;

namespace ApacheOrcDotNet {
    public interface IStatistics
    {
        void AnnotatePosition(long storedBufferOffset, long? decompressedOffset = null, int? rleValuesToConsume = null, int? bitsToConsume = null);
		void FillColumnStatistics(ColumnStatistics columnStatistics);
		void FillPositionList(List<ulong> positions, Func<int,bool> bufferIndexMustBeIncluded);
	}
}
