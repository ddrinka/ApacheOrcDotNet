using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Compression
{
	public class CompressionFactory : ICompressionFactory
	{
		public Stream CreateCompressorStream(CompressionType compressionType, CompressionStrategy compressionStrategy, Stream outputStream)
		{
			switch (compressionType)
			{
				case CompressionType.ZLIB: return new ZLibStream(compressionStrategy, outputStream);
				default:
					throw new NotImplementedException($"Unimplemented {nameof(CompressionType)} {compressionType}");
			}
		}

		public Stream CreateDecompressorStream(CompressionType compressionType, Stream inputStream)
		{
			switch (compressionType)
			{
				case CompressionType.ZLIB: return new ZLibStream(inputStream);
				default:
					throw new NotImplementedException($"Unimplemented {nameof(CompressionType)} {compressionType}");
			}
		}
	}
}
