using System;
using System.Collections.Generic;
using System.IO;
using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.FluentSerialization;
using ApacheOrcDotNet.Infrastructure;
using ApacheOrcDotNet.Stripes;

namespace ApacheOrcDotNet
{
	public class OrcWriter : IOrcWriter
	{
		readonly Stream _outputStream;
		readonly OrcCompressedBufferFactory _bufferFactory;
		readonly StripeWriter _stripeWriter;

		readonly List<uint> _version = new List<uint> { 0, 12 };
		readonly uint _writerVersion = 5;
		readonly string _magic = "ORC";

		public OrcWriter(Type type, Stream outputStream, WriterConfiguration configuration, SerializationConfiguration serializationConfiguration = null)
		{
			_outputStream = outputStream;

			_bufferFactory = new OrcCompressedBufferFactory(configuration);
			_stripeWriter = new StripeWriter(
				type,
				outputStream,
				configuration.EncodingStrategy == EncodingStrategy.Speed,
				configuration.DictionaryKeySizeThreshold,
				configuration.DefaultDecimalPrecision,
				configuration.DefaultDecimalScale,
				_bufferFactory,
				configuration.RowIndexStride,
				configuration.StripeSize,
				serializationConfiguration
				);

			WriteHeader();
		}

		public void AddRow(object row)
		{
			_stripeWriter.AddRows(new[] { row });
		}

		public void AddRows(IEnumerable<object> rows)
		{
			_stripeWriter.AddRows(rows);
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

			long metadataLength, footerLength;
			_bufferFactory.SerializeAndCompressTo(_outputStream, metadata, out metadataLength);
			_bufferFactory.SerializeAndCompressTo(_outputStream, footer, out footerLength);

			var postScript = GetPostscript((ulong)footerLength, (ulong)metadataLength);
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
