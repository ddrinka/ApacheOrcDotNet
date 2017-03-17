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
		public static void SerializeAndCompressTo(this OrcCompressedBufferFactory bufferFactory, Stream outputStream, object instance, out long length)
		{
			var buffer = bufferFactory.CreateBuffer();
			StaticProtoBuf.Serializer.Serialize(buffer, instance);
			buffer.CopyTo(outputStream);
			length = buffer.Length;
		}
	}
}
