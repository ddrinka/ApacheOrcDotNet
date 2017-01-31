using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Compression
{
	using IOStream = System.IO.Stream;

    public static class OrcCompressedStream
    {
		/// <summary>
		/// Provides a Stream that when read from, reads consecutive blocks of compressed data from an ORC Stream.
		/// All data in the <paramref name="inputStream"/> will be consumed.
		/// </summary>
		public static IOStream GetDecompressingStream(IOStream inputStream, CompressionKind compressionKind)
		{
			if (compressionKind == CompressionKind.None)
				return inputStream;
			else
				return new ConcatenatingStream(() =>
				{
					int blockLength;
					bool isCompressed;
					bool headerAvailable = ReadBlockHeader(inputStream, out blockLength, out isCompressed);
					if (!headerAvailable)
						return null;

					var streamSegment = new StreamSegment(inputStream, blockLength, true);

					if (!isCompressed)
						return streamSegment;
					else
						return CompressionFactory.CreateDecompressorStream(compressionKind, streamSegment);
				}, false);
		}

		static bool ReadBlockHeader(IOStream inputStream, out int blockLength, out bool isCompressed)
		{
			var firstByte = inputStream.ReadByte();
			if (firstByte < 0)      //End of stream
			{
				blockLength = 0;
				isCompressed = false;
				return false;
			}

			//From here it's a data error if the stream ends
			var rawValue = firstByte | inputStream.CheckedReadByte() << 8 | inputStream.CheckedReadByte() << 16;
			blockLength = rawValue >> 1;
			isCompressed = (rawValue & 1) == 0;

			return true;
		}
	}
}
