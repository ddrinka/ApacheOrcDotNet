using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace ApacheOrcDotNet.Test.ColumnTypes
{
    public class BinaryColumn_Test
    {
		[Fact]
		public void RoundTrip_BinaryColumn()
		{
			RoundTripSingleBinary(70000);
		}

		void RoundTripSingleBinary(int numValues)
		{
			var pocos = new List<SingleBinaryPoco>();
			var random = new Random(123);
			for (int i = 0; i < numValues; i++)
			{
				var numBytes = i % 100;
				byte[] bytes = new byte[numBytes];
				random.NextBytes(bytes);
				pocos.Add(new SingleBinaryPoco { Bytes = bytes });
			}

			var stream = new MemoryStream();
			Footer footer;
			StripeStreamHelper.Write(stream, pocos, out footer);
			var stripeStreams = StripeStreamHelper.GetStripeStreams(stream, footer);
			var binaryReader = new ApacheOrcDotNet.ColumnTypes.BinaryReader(stripeStreams, 1);
			var results = binaryReader.Read().ToArray();

			for (int i = 0; i < numValues; i++)
				Assert.True(pocos[i].Bytes.SequenceEqual(results[i]));
		}

		class SingleBinaryPoco
		{
			public byte[] Bytes { get; set; }
		}
	}
}
