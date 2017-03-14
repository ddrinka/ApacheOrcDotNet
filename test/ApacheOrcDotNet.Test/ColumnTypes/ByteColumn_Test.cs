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
    public class ByteColumn_Test
    {
		[Fact]
		public void RoundTrip_ByteColumn()
		{
			RoundTripSingleByte(70000);
		}

		void RoundTripSingleByte(int numValues)
		{
			var pocos = new List<SingleBytePoco>();
			var random = new Random(123);
			for (int i = 0; i < numValues; i++)
				pocos.Add(new SingleBytePoco { Byte = (byte)random.Next() });

			var stream = new MemoryStream();
			Footer footer;
			StripeStreamHelper.Write(stream, pocos, out footer);
			var stripeStreams = StripeStreamHelper.GetStripeStreams(stream, footer);
			var boolReader = new ByteReader(stripeStreams, 1);
			var results = boolReader.Read().ToArray();

			for (int i = 0; i < numValues; i++)
				Assert.Equal(pocos[i].Byte, results[i]);
		}

		class SingleBytePoco
		{
			public byte Byte { get; set; }
		}
    }
}
