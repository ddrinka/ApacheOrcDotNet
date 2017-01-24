using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Compression
{
	public class ZLib : ICompressor, IDecompressor
	{
		readonly CompressionLevel _compressionLevel;
		public ZLib(CompressionStrategy strategy)
		{
			switch (strategy)
			{
				case CompressionStrategy.Size: _compressionLevel = CompressionLevel.Optimal; break;
				case CompressionStrategy.Speed: _compressionLevel = CompressionLevel.Fastest; break;
				default: throw new NotImplementedException($"Unhandled {nameof(CompressionStrategy)} {strategy}");
			}
		}

		public byte[] Compress(byte[] inputBuffer, int offset)
		{
			var result = new MemoryStream();
			using (var deflateStream = new DeflateStream(result, _compressionLevel))
			{
				deflateStream.Write(inputBuffer, offset, inputBuffer.Length - offset);
			}
			return result.ToArray();
		}

		public byte[] Decompress(byte[] inputBuffer, int offset)
		{
			var result = new MemoryStream();
			using (var inputStream = new MemoryStream(inputBuffer, offset, inputBuffer.Length - offset))
			using (var inflateStream = new DeflateStream(inputStream, CompressionMode.Decompress))
			{
				inflateStream.CopyTo(result);
			}
			return result.ToArray();
		}
	}
}
