using ApacheOrcDotNet.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
	using Encodings;
	using System.IO;
	using System.Numerics;
	using IOStream = System.IO.Stream;

	public class ColumnReader
	{
		readonly StripeStreamCollection _stripeStreams;
		readonly uint _columnId;

		public ColumnReader(StripeStreamCollection stripeStreams, uint columnId)
		{
			_stripeStreams = stripeStreams;
			_columnId = columnId;
		}

		StripeStream GetStripeStream(StreamKind streamKind)
		{
			var stripeStream = _stripeStreams.FirstOrDefault(s => s.ColumnId == _columnId && s.StreamKind == streamKind);
			if (stripeStream == null)
				throw new InvalidOperationException($"Unabled to find stream for {nameof(_columnId)} ({_columnId}) and {nameof(streamKind)} ({streamKind})");

			return stripeStream;
		}

		protected long[] ReadNumericStream(StreamKind streamKind, bool isSigned)
		{
			var stripeStream = GetStripeStream(streamKind);
			if (stripeStream.ColumnEncodingKind != ColumnEncodingKind.DirectV2)
				throw new NotImplementedException($"Unimplemented Numeric {nameof(ColumnEncodingKind)} {stripeStream.ColumnEncodingKind}");

			var stream = stripeStream.GetDecompressedStream();
			var reader = new IntegerRunLengthEncodingV2Reader(stream, isSigned);

			return reader.Read().ToArray();
		}

		protected bool[] ReadBooleanStream(StreamKind streamKind)
		{
			var stripeStream = GetStripeStream(streamKind);
			var stream = stripeStream.GetDecompressedStream();
			var reader = new BitReader(stream);

			return reader.Read().ToArray();
		}

		protected byte[] ReadBinaryStream(StreamKind streamKind)
		{
			var stripeStream = GetStripeStream(streamKind);
			var stream = stripeStream.GetDecompressedStream();
			var memStream = new MemoryStream();
			stream.CopyTo(memStream);

			return memStream.ToArray();
		}

		protected byte[] ReadByteStream(StreamKind streamKind)
		{
			var stripeStream = GetStripeStream(streamKind);
			var stream = stripeStream.GetDecompressedStream();
			var reader = new ByteRunLengthEncodingReader(stream);

			return reader.Read().ToArray();
		}

		protected BigInteger[] ReadVarIntStream(StreamKind streamKind)
		{
			var stripeStream = GetStripeStream(streamKind);
			var stream = stripeStream.GetDecompressedStream();
			var reader = new VarIntReader(stream);

			return reader.Read().ToArray();
		}
	}
}
