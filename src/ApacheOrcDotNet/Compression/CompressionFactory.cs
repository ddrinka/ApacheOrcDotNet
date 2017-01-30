using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Compression
{
	using IOStream = System.IO.Stream;

	public static class CompressionFactory
	{
		/// <summary>
		/// Create a stream that when written to, writes compressed data to the provided <paramref name="outputStream"/>.
		/// </summary>
		/// <param name="compressionKind">Type of compression to use</param>
		/// <param name="compressionStrategy">Balance of speed vs. minimum data size</param>
		/// <param name="outputStream">Stream to write compressed data to</param>
		/// <returns>Writable compressing stream</returns>
		public static IOStream CreateCompressorStream(CompressionKind compressionType, CompressionStrategy compressionStrategy, IOStream outputStream)
		{
			switch (compressionType)
			{
				case CompressionKind.Zlib: return new ZLibStream(compressionStrategy, outputStream);
				default:
					throw new NotImplementedException($"Unimplemented {nameof(CompressionType)} {compressionType}");
			}
		}

		/// <summary>
		/// Create a stream that when read from, decompresses data from the provided <paramref name="inputStream"/>.
		/// </summary>
		/// <param name="compressionType">Type of compression to use</param>
		/// <param name="inputStream">Stream to read compressed data from</param>
		/// <returns>Readable decompressing stream</returns>
		public static IOStream CreateDecompressorStream(CompressionKind compressionType, IOStream inputStream)
		{
			switch (compressionType)
			{
				case CompressionKind.Zlib: return new ZLibStream(inputStream);
				default:
					throw new NotImplementedException($"Unimplemented {nameof(CompressionType)} {compressionType}");
			}
		}
	}
}
