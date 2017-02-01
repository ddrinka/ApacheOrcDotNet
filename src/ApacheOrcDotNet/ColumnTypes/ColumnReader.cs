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
		readonly StripeStreamReaderCollection _stripeStreams;
		readonly uint _columnId;

		public ColumnReader(StripeStreamReaderCollection stripeStreams, uint columnId)
		{
			_stripeStreams = stripeStreams;
			_columnId = columnId;
		}

		StripeStreamReader GetStripeStream(StreamKind streamKind)
		{
			return _stripeStreams.FirstOrDefault(s => s.ColumnId == _columnId && s.StreamKind == streamKind);
		}

		protected ColumnEncodingKind? GetColumnEncodingKind(StreamKind streamKind)
		{
			var stripeStream = GetStripeStream(streamKind);
			if (stripeStream == null)
				return null;
			return stripeStream.ColumnEncodingKind;
		}

		protected long[] ReadNumericStream(StreamKind streamKind, bool isSigned)
		{
			var stripeStream = GetStripeStream(streamKind);
			if (stripeStream == null)
				return null;

			if (stripeStream.ColumnEncodingKind != ColumnEncodingKind.DirectV2)
				throw new NotImplementedException($"Unimplemented Numeric {nameof(ColumnEncodingKind)} {stripeStream.ColumnEncodingKind}");

			var stream = stripeStream.GetDecompressedStream();
			var reader = new IntegerRunLengthEncodingV2Reader(stream, isSigned);

			return reader.Read().ToArray();
		}

		protected bool[] ReadBooleanStream(StreamKind streamKind)
		{
			var stripeStream = GetStripeStream(streamKind);
			if (stripeStream == null)
				return null;

			var stream = stripeStream.GetDecompressedStream();
			var reader = new BitReader(stream);

			return reader.Read().ToArray();
		}

		protected byte[] ReadBinaryStream(StreamKind streamKind)
		{
			var stripeStream = GetStripeStream(streamKind);
			if (stripeStream == null)
				return null;

			var stream = stripeStream.GetDecompressedStream();
			var memStream = new MemoryStream();
			stream.CopyTo(memStream);

			return memStream.ToArray();
		}

		protected byte[] ReadByteStream(StreamKind streamKind)
		{
			var stripeStream = GetStripeStream(streamKind);
			if (stripeStream == null)
				return null;

			var stream = stripeStream.GetDecompressedStream();
			var reader = new ByteRunLengthEncodingReader(stream);

			return reader.Read().ToArray();
		}

		protected BigInteger[] ReadVarIntStream(StreamKind streamKind)
		{
			var stripeStream = GetStripeStream(streamKind);
			if (stripeStream == null)
				return null;

			var stream = stripeStream.GetDecompressedStream();
			var reader = new VarIntReader(stream);

			return reader.Read().ToArray();
		}
	}
}
