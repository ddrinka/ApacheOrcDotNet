using ApacheOrcDotNet.Stripes;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
	public class TimestampReader : ColumnReader
	{
		readonly static DateTime _orcEpoch = new DateTime(2015, 1, 1, 0, 0, 0, DateTimeKind.Utc);

		public TimestampReader(StripeStreamReaderCollection stripeStreams, uint columnId) : base(stripeStreams, columnId)
		{
		}

		public IEnumerable<DateTime?> Read()
		{
			var present = ReadBooleanStream(Protocol.StreamKind.Present);
			var data = ReadNumericStream(Protocol.StreamKind.Data, true);
			var secondary = ReadNumericStream(Protocol.StreamKind.Secondary, false);
			if (data == null || secondary == null)
				throw new InvalidDataException("DATA and SECONDARY streams must be available");

			var dataEnumerator = ((IEnumerable<long>)data).GetEnumerator();
			var secondaryEnumerator = ((IEnumerable<long>)secondary).GetEnumerator();
			if (present == null)
			{
				while (dataEnumerator.MoveNext() && secondaryEnumerator.MoveNext())
				{
					var seconds = dataEnumerator.Current;
					var nanosecondTicks = EncodedNanosToTicks(secondaryEnumerator.Current);
					var totalTicks = seconds * TimeSpan.TicksPerSecond + (seconds >= 0 ? nanosecondTicks : -nanosecondTicks);
					yield return _orcEpoch.AddTicks(totalTicks);
				}
			}
			else
			{
				foreach (var isPresent in present)
				{
					if (isPresent)
					{
						var success = dataEnumerator.MoveNext() && secondaryEnumerator.MoveNext();
						if (!success)
							throw new InvalidDataException("The PRESENT data stream's length didn't match the DATA and SECONDARY streams' lengths");

						var seconds = dataEnumerator.Current;
						var nanosecondTicks = EncodedNanosToTicks(secondaryEnumerator.Current);
						var totalTicks = seconds * TimeSpan.TicksPerSecond + (seconds >= 0 ? nanosecondTicks : -nanosecondTicks);
						yield return _orcEpoch.AddTicks(totalTicks);
					}
					else
						yield return null;
				}
			}
		}

		long EncodedNanosToTicks(long encodedNanos)
		{
			var scale = (int)(encodedNanos & 0x7);
			var nanos = encodedNanos >> 3;

			if (scale == 0)
				return nanos;

			while (scale-- >= 0)
				nanos *= 10;

			return nanos / 100;		//100 nanoseconds per tick
		}
	}
}
