using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ApacheOrcDotNet.Protocol;

namespace ApacheOrcDotNet.Compression
{
	public class OrcCompressedBufferFactory
	{
		readonly int _compressionBlockSize;
		readonly CompressionKind _compressionKind;
		readonly CompressionStrategy _compressionStrategy;

		public OrcCompressedBufferFactory(WriterConfiguration configuration)
		{
			_compressionBlockSize = configuration.BufferSize;
			_compressionKind = configuration.Compress.ToCompressionKind();
			_compressionStrategy = configuration.CompressionStrategy;
		}

		public OrcCompressedBufferFactory(int compressionBlockSize, CompressionKind compressionKind, CompressionStrategy compressionStrategy)
		{
			_compressionBlockSize = compressionBlockSize;
			_compressionKind = compressionKind;
			_compressionStrategy = compressionStrategy;
		}

		public OrcCompressedBuffer CreateBuffer()
		{
			return new OrcCompressedBuffer(_compressionBlockSize, _compressionKind, _compressionStrategy);
		}
    }
}
