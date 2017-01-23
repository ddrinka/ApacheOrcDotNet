using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Compression
{
	public class CompressionFactory : ICompressionFactory
	{
		public ICompressor CreateCompressor(CompressionType compressionType, CompressionStrategy compressionStrategy)
		{
			switch(compressionType)
			{
				case CompressionType.ZLIB:return new ZLib(compressionStrategy);
				default:
					throw new NotImplementedException($"Unimplemented {nameof(CompressionType)} {compressionType}");
			}
		}
	}
}
