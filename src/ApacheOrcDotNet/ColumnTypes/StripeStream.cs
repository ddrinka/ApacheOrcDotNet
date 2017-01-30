using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Infrastructure;
using ApacheOrcDotNet.Protocol;


namespace ApacheOrcDotNet.ColumnTypes
{
	using IOStream = System.IO.Stream;
	using OrcStream = ApacheOrcDotNet.Protocol.Stream;

	public class StripeStream
    {
		readonly IOStream _inputStream;
		readonly long _inputStreamOffset;
		readonly ulong _compressedLength;
		readonly CompressionKind _compressionKind;

		internal StripeStream(IOStream inputStream, uint columnId, StreamKind streamKind, ColumnEncodingKind encodingKind, long inputStreamOffset, ulong compressedLength, CompressionKind compressionKind)
		{
			_inputStream = inputStream;
			ColumnId = columnId;
			StreamKind = streamKind;
			_inputStreamOffset = inputStreamOffset;
			_compressedLength = compressedLength;
			_compressionKind = compressionKind;
		}

		public uint ColumnId { get; }
		public StreamKind StreamKind { get; }
		public ColumnEncodingKind ColumnEncodingKind { get; }

		public IOStream GetDecompressedStream()
		{
			//TODO move from using Streams to using MemoryMapped files or another data type that decouples the Stream Position from the Read call, allowing re-entrancy
			_inputStream.Seek(_inputStreamOffset, System.IO.SeekOrigin.Begin);
			var segment = new StreamSegment(_inputStream, (long)_compressedLength, true);
			if (_compressionKind == CompressionKind.None)
				return segment;
			else
				return OrcCompressedStream.GetDecompressingStream(segment, _compressionKind);
		}
	}
}
