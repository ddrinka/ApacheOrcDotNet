using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet
{
    public interface IStatistics<T>
    {
		void ProcessValues(ArraySegment<T> value);
		void AnnotatePosition(long compressedBlockOffset, long decompressedOffset, long rleValuesToConsume);
    }
}
