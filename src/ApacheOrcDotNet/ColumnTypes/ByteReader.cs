using ApacheOrcDotNet.Stripes;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class ByteReader : ColumnReader
    {
		public ByteReader(StripeStreamReaderCollection stripeStreams, uint columnId) : base(stripeStreams, columnId)
		{
		}

		public IEnumerable<byte?> Read()
		{
			var present = ReadBooleanStream(Protocol.StreamKind.Present);
			var data = ReadByteStream(Protocol.StreamKind.Data);
			if (present == null)
			{
				foreach (var value in data)
					yield return value;
			}
			else
			{
				var valueEnumerator = ((IEnumerable<byte>)data).GetEnumerator();
				foreach (var isPresent in present)
				{
					if (isPresent)
					{
						var success = valueEnumerator.MoveNext();
						if (!success)
							throw new InvalidDataException("The PRESENT data stream's length didn't match the DATA stream's length");
						yield return valueEnumerator.Current;
					}
					else
						yield return null;
				}
			}
		}
	}
}
