using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Compression
{
	public interface ICompressionFactory
    {
		/// <summary>
		/// Create a stream that when written to, writes compressed data to the provided <paramref name="outputStream"/>.
		/// </summary>
		/// <param name="compressionType">Type of compression to use</param>
		/// <param name="compressionStrategy">Balance of speed vs. minimum data size</param>
		/// <param name="outputStream">Stream to write compressed data to</param>
		/// <returns>Writable compressing stream</returns>
		Stream CreateCompressorStream(CompressionType compressionType, CompressionStrategy compressionStrategy, Stream outputStream);

		/// <summary>
		/// Create a stream that when read from, decompresses data from the provided <paramref name="inputStream"/>.
		/// </summary>
		/// <param name="compressionType">Type of compression to use</param>
		/// <param name="inputStream">Stream to read compressed data from</param>
		/// <returns>Readable decompressing stream</returns>
		Stream CreateDecompressorStream(CompressionType compressionType, Stream inputStream);
    }
}
