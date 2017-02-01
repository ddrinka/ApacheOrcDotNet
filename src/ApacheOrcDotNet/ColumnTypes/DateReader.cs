using ApacheOrcDotNet.Stripes;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
	public class DateReader : ColumnReader
	{
		readonly static DateTime _unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

		public DateReader(StripeStreamReaderCollection stripeStreams, uint columnId) : base(stripeStreams, columnId)
		{
		}

		public IEnumerable<DateTime?> Read()
		{
			var present = ReadBooleanStream(Protocol.StreamKind.Present);
			var data = ReadNumericStream(Protocol.StreamKind.Data, true);
			if (present == null)
			{
				foreach (var value in data)
					yield return _unixEpoch.AddTicks(value * TimeSpan.TicksPerDay);
			}
			else
			{
				var valueEnumerator = ((IEnumerable<long>)data).GetEnumerator();
				foreach (var isPresent in present)
				{
					if (isPresent)
					{
						var success = valueEnumerator.MoveNext();
						if (!success)
							throw new InvalidDataException("The PRESENT data stream's length didn't match the DATA stream's length");
						yield return _unixEpoch.AddTicks(valueEnumerator.Current * TimeSpan.TicksPerDay);
					}
					else
						yield return null;
				}
			}
		}
	}
}
