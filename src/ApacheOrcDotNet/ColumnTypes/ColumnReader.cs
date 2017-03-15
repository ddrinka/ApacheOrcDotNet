using ApacheOrcDotNet.Infrastructure;
using ApacheOrcDotNet.Stripes;
using ApacheOrcDotNet.Encodings;
using System;
using System.IO;
using System.Numerics;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
	public class ColumnReader
	{
		readonly StripeStreamReaderCollection _stripeStreams;
		readonly uint _columnId;

		public ColumnReader(StripeStreamReaderCollection stripeStreams, uint columnId)
		{
			_stripeStreams = stripeStreams;
			_columnId = columnId;
		}

		StripeStreamReader GetStripeStream(Protocol.StreamKind streamKind)
		{
			return _stripeStreams.FirstOrDefault(s => s.ColumnId == _columnId && s.StreamKind == streamKind);
		}

		protected Protocol.ColumnEncodingKind? GetColumnEncodingKind(Protocol.StreamKind streamKind)
		{
			var stripeStream = GetStripeStream(streamKind);
			if (stripeStream == null)
				return null;
			return stripeStream.ColumnEncodingKind;
		}

		protected long[] ReadNumericStream(Protocol.StreamKind streamKind, bool isSigned)
		{
			var stripeStream = GetStripeStream(streamKind);
			if (stripeStream == null)
				return null;

			if (stripeStream.ColumnEncodingKind != Protocol.ColumnEncodingKind.DirectV2 && stripeStream.ColumnEncodingKind != Protocol.ColumnEncodingKind.DictionaryV2)
				throw new NotImplementedException($"Unimplemented Numeric {nameof(Protocol.ColumnEncodingKind)} {stripeStream.ColumnEncodingKind}");

			var stream = stripeStream.GetDecompressedStream();
			var reader = new IntegerRunLengthEncodingV2Reader(stream, isSigned);

			return reader.Read().ToArray();
		}

		protected bool[] ReadBooleanStream(Protocol.StreamKind streamKind)
		{
			var stripeStream = GetStripeStream(streamKind);
			if (stripeStream == null)
				return null;

			var stream = stripeStream.GetDecompressedStream();
			var reader = new BitReader(stream);

			return reader.Read().ToArray();
		}

		protected byte[] ReadBinaryStream(Protocol.StreamKind streamKind)
		{
			var stripeStream = GetStripeStream(streamKind);
			if (stripeStream == null)
				return null;

			var stream = stripeStream.GetDecompressedStream();
			var memStream = new MemoryStream();
			stream.CopyTo(memStream);

			return memStream.ToArray();
		}

		protected byte[] ReadByteStream(Protocol.StreamKind streamKind)
		{
			var stripeStream = GetStripeStream(streamKind);
			if (stripeStream == null)
				return null;

			var stream = stripeStream.GetDecompressedStream();
			var reader = new ByteRunLengthEncodingReader(stream);

			return reader.Read().ToArray();
		}

		protected BigInteger[] ReadVarIntStream(Protocol.StreamKind streamKind)
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
