using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.Stripes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
	public class DoubleReader : ColumnReader
	{
		public DoubleReader(StripeStreamReaderCollection stripeStreams, uint columnId) : base(stripeStreams, columnId)
		{
		}

		public IEnumerable<double?> Read()
		{
			var present = ReadBooleanStream(Protocol.StreamKind.Present);
			var data = ReadBinaryStream(Protocol.StreamKind.Data);
			int dataIndex = 0;
			if (present == null)
			{
				while (dataIndex + 8 <= data.Length)
				{
					var value = BitManipulation.ReadDouble(data, dataIndex);
					dataIndex += 8;
					yield return value;
				}
			}
			else
			{
				foreach (var isPresent in present)
				{
					if (isPresent)
					{
						var value = BitManipulation.ReadDouble(data, dataIndex);
						dataIndex += 8;
						yield return value;
					}
					else
						yield return null;
				}
			}
		}
	}
}
