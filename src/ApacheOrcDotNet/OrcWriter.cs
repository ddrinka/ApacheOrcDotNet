using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Infrastructure;
using ApacheOrcDotNet.Stripes;
using ProtoBuf.Meta;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet
{
	public class OrcWriter<T> : IOrcWriter<T>
	{
		readonly Stream _outputStream;
		readonly OrcCompressedBufferFactory _bufferFactory;
		readonly StripeWriter _stripeWriter;

		readonly List<uint> _version = new List<uint> { 0, 12 };
		readonly uint _writerVersion = 5;
		readonly string _magic = "ORC";

		public OrcWriter(Stream outputStream, WriterConfiguration configuration)
		{
			_outputStream = outputStream;

			_bufferFactory = new OrcCompressedBufferFactory(configuration);
			_stripeWriter = new StripeWriter(
				typeof(T),
				outputStream,
				configuration.EncodingStrategy == EncodingStrategy.Speed,
				_bufferFactory,
				configuration.RowIndexStride,
				configuration.StripeSize
				);

			WriteHeader();
		}

		public void AddRow(T row)
		{
			_stripeWriter.AddRows(new object[] { row });
		}

		public void AddRows(IEnumerable<T> rows)
		{
			_stripeWriter.AddRows((IEnumerable<object>)rows);
		}

		public void AddUserMetadata(string key, byte[] value)
		{
			throw new NotImplementedException();
		}

		public void Dispose()
		{
			_stripeWriter.RowAddingCompleted();

			WriteTail();
		}

		void WriteTail()
		{
			var metadata = _stripeWriter.GetMetadata();
			var footer = _stripeWriter.GetFooter();
			footer.HeaderLength = (ulong)_magic.Length;

			var metadataStream = _bufferFactory.SerializeAndCompress(metadata);
			var footerStream = _bufferFactory.SerializeAndCompress(footer);
			metadataStream.CopyTo(_outputStream);
			footerStream.CopyTo(_outputStream);

			var postScript = GetPostscript((ulong)footerStream.Length, (ulong)metadataStream.Length);
			var postScriptStream = new MemoryStream();
			StaticProtoBuf.Serializer.Serialize(postScriptStream, postScript);
			postScriptStream.Seek(0, SeekOrigin.Begin);
			postScriptStream.CopyTo(_outputStream);

			if (postScriptStream.Length > 255)
				throw new InvalidDataException("Invalid Postscript length");

			_outputStream.WriteByte((byte)postScriptStream.Length);
		}

		Protocol.PostScript GetPostscript(ulong footerLength, ulong metadataLength)
		{
			return new Protocol.PostScript
			{
				FooterLength = footerLength,
				Compression = _bufferFactory.CompressionKind,
				CompressionBlockSize = (ulong)_bufferFactory.CompressionBlockSize,
				Version = _version,
				MetadataLength = metadataLength,
				WriterVersion = _writerVersion,
				Magic = _magic
			};
		}

		void WriteHeader()
		{
			var magic = new byte[] { (byte)'O', (byte)'R', (byte)'C' };
			_outputStream.Write(magic, 0, magic.Length);
		}
	}
}
