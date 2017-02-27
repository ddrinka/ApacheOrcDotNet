using ApacheOrcDotNet.Compression;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Infrastructure
{
    public static class OrcCompressedBufferFactoryExtensions
    {
		public static MemoryStream SerializeAndCompress(this OrcCompressedBufferFactory bufferFactory, object instance)
		{
			var buffer = bufferFactory.CreateBuffer();
			ProtoBuf.Serializer.Serialize(buffer, instance);
			buffer.WritingCompleted();
			return buffer.CompressedBuffer;
		}
	}
}
