using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Infrastructure;
using System.IO;

namespace ApacheOrcDotNet.Stripes
{
	public class StripeStreamReader
    {
		readonly Stream _inputStream;
		readonly long _inputStreamOffset;
		readonly ulong _compressedLength;
		readonly Protocol.CompressionKind _compressionKind;

		internal StripeStreamReader(Stream inputStream, uint columnId, Protocol.StreamKind streamKind, Protocol.ColumnEncodingKind encodingKind, long inputStreamOffset, ulong compressedLength, Protocol.CompressionKind compressionKind)
		{
			_inputStream = inputStream;
			ColumnId = columnId;
			StreamKind = streamKind;
			ColumnEncodingKind = encodingKind;
			_inputStreamOffset = inputStreamOffset;
			_compressedLength = compressedLength;
			_compressionKind = compressionKind;
		}

		public uint ColumnId { get; }
		public Protocol.StreamKind StreamKind { get; }
		public Protocol.ColumnEncodingKind ColumnEncodingKind { get; }

		public Stream GetDecompressedStream()
		{
			//TODO move from using Streams to using MemoryMapped files or another data type that decouples the Stream Position from the Read call, allowing re-entrancy
			_inputStream.Seek(_inputStreamOffset, System.IO.SeekOrigin.Begin);
			var segment = new StreamSegment(_inputStream, (long)_compressedLength, true);
			return OrcCompressedStream.GetDecompressingStream(segment, _compressionKind);
		}
	}
}
