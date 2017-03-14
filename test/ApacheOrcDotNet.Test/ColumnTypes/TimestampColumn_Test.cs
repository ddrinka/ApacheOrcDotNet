using ApacheOrcDotNet.ColumnTypes;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace ApacheOrcDotNet.Test.ColumnTypes
{
    public class TimestampColumn_Test
    {
		[Fact]
		public void RoundTrip_TimestampColumn()
		{
			RoundTripSingleValue(70000);
		}

		void RoundTripSingleValue(int numValues)
		{
			var random = new Random(123);
			var pocos = GenerateRandomTimestamps(random, numValues).Select(t => new SingleValuePoco { Value = t }).ToList();

			var stream = new MemoryStream();
			Footer footer;
			StripeStreamHelper.Write(stream, pocos, out footer);
			var stripeStreams = StripeStreamHelper.GetStripeStreams(stream, footer);
			var reader = new TimestampReader(stripeStreams, 1);
			var results=reader.Read().ToArray();

			for (int i = 0; i < numValues; i++)
				Assert.Equal(pocos[i].Value, results[i]);
		}

		enum Precision { Nanos, Micros, Millis, Seconds }
		IEnumerable<DateTime> GenerateRandomTimestamps(Random rnd, int count)
		{
			var baseTime = new DateTime(2015, 1, 1, 0, 0, 0, DateTimeKind.Utc);
			for(int i=0;i<count;i++)
			{
				Precision p = (Precision)(rnd.Next() % 4);
				switch (p)
				{
					case Precision.Seconds:yield return baseTime.AddSeconds(GetSeconds(rnd));break;
					case Precision.Millis:yield return baseTime.AddSeconds(GetSeconds(rnd)).AddTicks(GetMillisecondTicks(rnd));break;
					case Precision.Micros:yield return baseTime.AddSeconds(GetSeconds(rnd)).AddTicks(GetMicrosecondTicks(rnd));break;
					case Precision.Nanos:yield return baseTime.AddSeconds(GetSeconds(rnd)).AddTicks(GetNanosecondTicks(rnd));break;
				}
			}
		}

		int GetSeconds(Random rnd)
		{
			var seconds = 10 * 365 * 24 * 60 * 60;  //10 years
			return (rnd.Next() % seconds) - (seconds / 2);    //A positive or negative random number
		}

		long GetMillisecondTicks(Random rnd)
		{
			return (rnd.Next() % 1000)*TimeSpan.TicksPerMillisecond;
		}

		long GetMicrosecondTicks(Random rnd)
		{
			return (rnd.Next() % (1000 * 1000)) * (TimeSpan.TicksPerMillisecond / 1000);
		}

		long GetNanosecondTicks(Random rnd)
		{
			return rnd.Next() % (1000 * 1000 * 1000 / 100);
		}

		class SingleValuePoco
		{
			public DateTime Value { get; set; }
		}
    }
}
