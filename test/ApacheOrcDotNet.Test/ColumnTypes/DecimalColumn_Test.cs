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
    public class DecimalColumn_Test
    {
		[Fact]
		public void RoundTrip_DecimalColumn()
		{
			RoundTripSingleValue(70000);
		}

		void RoundTripSingleValue(int numValues)
		{
			var pocos = new List<SingleValuePoco>();
			var random = new Random(123);
			for (int i = 0; i < numValues; i++)
				pocos.Add(new SingleValuePoco { Value = (decimal)random.Next() / (decimal)Math.Pow(10, random.Next() % 10) });

			var stream = new MemoryStream();
			Footer footer;
			StripeStreamHelper.Write(stream, pocos, out footer);
			var stripeStreams = StripeStreamHelper.GetStripeStreams(stream, footer);
			var reader = new DecimalReader(stripeStreams, 1);
			var results = reader.Read().ToArray();

			for (int i = 0; i < numValues; i++)
				Assert.Equal(pocos[i].Value, results[i]);
		}

		class SingleValuePoco
		{
			public decimal Value { get; set; }
		}
    }
}
