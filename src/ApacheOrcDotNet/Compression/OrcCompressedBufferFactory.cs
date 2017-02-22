using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.IO;

namespace ApacheOrcDotNet.Compression
{
	public class OrcCompressedBufferFactory
	{
		readonly int _compressionBlockSize;
		readonly Protocol.CompressionKind _compressionKind;
		readonly CompressionStrategy _compressionStrategy;

		public OrcCompressedBufferFactory(WriterConfiguration configuration)
		{
			_compressionBlockSize = configuration.BufferSize;
			_compressionKind = configuration.Compress.ToCompressionKind();
			_compressionStrategy = configuration.CompressionStrategy;
		}

		public OrcCompressedBufferFactory(int compressionBlockSize, Protocol.CompressionKind compressionKind, CompressionStrategy compressionStrategy)
		{
			_compressionBlockSize = compressionBlockSize;
			_compressionKind = compressionKind;
			_compressionStrategy = compressionStrategy;
		}

		public OrcCompressedBuffer CreateBuffer(Protocol.StreamKind streamKind)
		{
			return new OrcCompressedBuffer(_compressionBlockSize, _compressionKind, _compressionStrategy, streamKind);
		}
    }
}
