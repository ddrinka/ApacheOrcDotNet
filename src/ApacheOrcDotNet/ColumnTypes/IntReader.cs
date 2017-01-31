using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
	public class IntReader : ColumnReader
	{
		public IntReader(StripeStreamCollection stripeStreams, uint columnId) : base(stripeStreams, columnId)
		{
		}

		public IEnumerable<int?> Read()
		{
			var present = ReadBooleanStream(Protocol.StreamKind.Present);
			var data = ReadNumericStream(Protocol.StreamKind.Data, true);
			if (present == null)
			{
				foreach (var value in data)
					yield return (int)value;
			}
			else
			{
				var valueEnumerator = ((IEnumerable<long>)data).GetEnumerator();
				foreach(var isPresent in present)
				{
					if (isPresent)
					{
						var success = valueEnumerator.MoveNext();
						if (!success)
							throw new InvalidDataException("The PRESENT data stream's length didn't match the DATA stream's length");
						yield return (int)valueEnumerator.Current;
					}
					else
						yield return null;
				}
			}
		}
	}
}
