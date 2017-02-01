using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Infrastructure;
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Stripes
{
    public class StripeReader
    {
		readonly Stream _inputStream;
		readonly ulong _indexOffset;
		readonly ulong _indexLength;
		readonly ulong _dataOffset;
		readonly ulong _dataLength;
		readonly ulong _footerOffset;
		readonly ulong _footerLength;
		readonly Protocol.CompressionKind _compressionKind;

		internal StripeReader(Stream inputStream, ulong indexOffset, ulong indexLength, ulong dataOffset, ulong dataLength, ulong footerOffset, ulong footerLength, ulong numRows, Protocol.CompressionKind compressionKind)
		{
			_inputStream = inputStream;
			_indexOffset = indexOffset;
			_indexLength = indexLength;
			_dataOffset = dataOffset;
			_dataLength = dataLength;
			_footerOffset = footerOffset;
			_footerLength = footerLength;
			NumRows = numRows;
			_compressionKind = compressionKind;
		}

		public ulong NumRows { get; }

		Stream GetStream(ulong offset, ulong length)
		{
			//TODO move from using Streams to using MemoryMapped files or another data type that decouples the Stream Position from the Read call, allowing re-entrancy
			_inputStream.Seek((long)offset, SeekOrigin.Begin);
			var segment = new StreamSegment(_inputStream, (long)length, true);
			return OrcCompressedStream.GetDecompressingStream(segment, _compressionKind);
		}

		Protocol.StripeFooter GetStripeFooter()
		{
			var stream = GetStream(_footerOffset, _footerLength);
			return Serializer.Deserialize<Protocol.StripeFooter>(stream);
		}

		public Stream GetIndexStream()
		{
			return GetStream(_indexOffset, _indexLength);
		}

		public StripeStreamReaderCollection GetStripeStreamCollection()
		{
			var footer = GetStripeFooter();
			return new StripeStreamReaderCollection(_inputStream, footer, (long)_indexOffset, _compressionKind);
		}
	}
}
