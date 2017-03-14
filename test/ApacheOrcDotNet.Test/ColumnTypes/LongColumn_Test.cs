using ApacheOrcDotNet.ColumnTypes;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace ApacheOrcDotNet.Test.ColumnTypes
{
    public class LongColumn_Test
    {
		[Fact]
		public void RoundTrip_LongColumn()
		{
			RoundTripSingleInt(70000);
		}

		void RoundTripSingleInt(int numValues)
		{
			var pocos = new List<SingleIntPoco>();
			var random = new Random(123);
			for (int i = 0; i < numValues; i++)
				pocos.Add(new SingleIntPoco { Int = random.Next() });

			var stream = new MemoryStream();
			Footer footer;
			StripeStreamHelper.Write(stream, pocos, out footer);
			var stripeStreams = StripeStreamHelper.GetStripeStreams(stream, footer);
			var longReader = new LongReader(stripeStreams, 1);
			var results = longReader.Read().ToArray();

			for (int i = 0; i < numValues; i++)
				Assert.Equal(pocos[i].Int, results[i]);
		}

		class SingleIntPoco
		{
			public int Int { get; set; }
		}
	}
}
