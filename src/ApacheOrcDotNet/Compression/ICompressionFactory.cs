using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Compression
{
	public interface ICompressionFactory
    {
		ICompressor CreateCompressor(CompressionType compressionType, CompressionStrategy compressionStrategy);
    }
}
