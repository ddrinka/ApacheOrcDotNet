using ApacheOrcDotNet.ColumnTypes;
using ApacheOrcDotNet.Test.TestHelpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace ApacheOrcDotNet.Test.ColumnTypes
{
    public class SmallIntReader_Test
    {
		StripeStreamCollection GetStripeStreamCollection()
		{
			var dataFile = new DataFileHelper("demo-12-zlib.orc");
			var stream = dataFile.GetStream();
			var fileTail = new FileTail(stream);
			var stripes = fileTail.GetStripeCollection();
			Assert.Equal(1, stripes.Count);
			return stripes[0].GetStripeStreamCollection();
		}

		[Fact]
		public void ReadColumn7_ShouldProduceExpectedResults()
		{
			var stripeStreams = GetStripeStreamCollection();
			var smallIntReader = new SmallIntReader(stripeStreams, 7);
			var results = smallIntReader.Read().ToArray();

			Assert.Equal(1920800, results.Length);
			for(int i=0;i<results.Length;i++)
			{
				var expected = (i / 5600) % 7;
				Assert.True(results[i].HasValue);
				Assert.Equal(expected, results[i].Value);
			}
		}

		[Fact]
		public void ReadColumn8_ShouldProduceExpectedResults()
		{
			var stripeStreams = GetStripeStreamCollection();
			var smallIntReader = new SmallIntReader(stripeStreams, 8);
			var results = smallIntReader.Read().ToArray();

			Assert.Equal(1920800, results.Length);
			for (int i = 0; i < results.Length; i++)
			{
				var expected = (i / 39200) % 7;
				Assert.True(results[i].HasValue);
				Assert.Equal(expected, results[i].Value);
			}
		}

		[Fact]
		public void ReadColumn9_ShouldProduceExpectedResults()
		{
			var stripeStreams = GetStripeStreamCollection();
			var smallIntReader = new SmallIntReader(stripeStreams, 9);
			var results = smallIntReader.Read().ToArray();

			Assert.Equal(1920800, results.Length);
			for (int i = 0; i < results.Length; i++)
			{
				var expected = (i / 274400);
				Assert.True(results[i].HasValue);
				Assert.Equal(expected, results[i].Value);
			}
		}
	}
}
