using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.IO;
using ApacheOrcDotNet.Protocol;

namespace ApacheOrcDotNet.Compression
{
	public class OrcCompressedBufferFactory
	{
		public int CompressionBlockSize { get; }
		public CompressionKind CompressionKind { get; }
		public CompressionStrategy CompressionStrategy { get; }

		public OrcCompressedBufferFactory(WriterConfiguration configuration)
		{
			CompressionBlockSize = configuration.BufferSize;
			CompressionKind = configuration.Compress.ToCompressionKind();
			CompressionStrategy = configuration.CompressionStrategy;
		}

		public OrcCompressedBufferFactory(int compressionBlockSize, Protocol.CompressionKind compressionKind, CompressionStrategy compressionStrategy)
		{
			CompressionBlockSize = compressionBlockSize;
			CompressionKind = compressionKind;
			CompressionStrategy = compressionStrategy;
		}

		public OrcCompressedBuffer CreateBuffer(Protocol.StreamKind streamKind)
		{
			return new OrcCompressedBuffer(CompressionBlockSize, CompressionKind, CompressionStrategy, streamKind);
		}

		public OrcCompressedBuffer CreateBuffer()
		{
			return new OrcCompressedBuffer(CompressionBlockSize, CompressionKind, CompressionStrategy);
		}
    }
}
