using ApacheOrcDotNet.Compression;
using System.IO;

namespace ApacheOrcDotNet.Infrastructure {
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
