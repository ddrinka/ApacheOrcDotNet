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
    public class BooleanColumn_Test
    {
		[Fact]
		public void RoundTrip_BooleanColumn()
		{
			RoundTripSingleBool(70000);
		}

		void RoundTripSingleBool(int numValues)
		{
			var pocos = new List<SingleBoolPoco>();
			var random = new Random(123);
			for (int i = 0; i < numValues; i++)
				pocos.Add(new SingleBoolPoco { Bool = random.Next() % 2 == 0 });

			var stream = new MemoryStream();
			Footer footer;
			StripeStreamHelper.Write(stream, pocos, out footer);
			var stripeStreams = StripeStreamHelper.GetStripeStreams(stream, footer);
			var boolReader = new BooleanReader(stripeStreams, 1);
			var results = boolReader.Read().ToArray();

			for (int i = 0; i < numValues; i++)
				Assert.Equal(pocos[i].Bool, results[i]);
		}

		class SingleBoolPoco
		{
			public bool Bool { get; set; }
		}
	}
}
