using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet
{
    public interface IStatistics<T>
    {
		void AnnotatePosition(long compressedBufferOffset, long decompressedOffset, long rleValuesToConsume);
    }
}
