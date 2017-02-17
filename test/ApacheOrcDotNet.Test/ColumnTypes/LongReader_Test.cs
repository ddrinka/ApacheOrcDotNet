using ApacheOrcDotNet.ColumnTypes;
using ApacheOrcDotNet.Stripes;
using ApacheOrcDotNet.Test.TestHelpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace ApacheOrcDotNet.Test.ColumnTypes
{
    public class LongReader_Test
    {
		StripeStreamReaderCollection GetStripeStreamCollection()
		{
			var dataFile = new DataFileHelper("demo-12-zlib.orc");
			var stream = dataFile.GetStream();
			var fileTail = new FileTail(stream);
			var stripes = fileTail.GetStripeCollection();
			Assert.Equal(1, stripes.Count);
			return stripes[0].GetStripeStreamCollection();
		}

		[Fact]
		public void ReadColumn1_ShouldProduceExpectedResults()
		{
			var stripeStreams = GetStripeStreamCollection();
			var longReader = new LongReader(stripeStreams, 1);
			var results = longReader.Read().ToArray();

			Assert.Equal(1920800, results.Length);
			for (int i = 0; i < results.Length; i++)
			{
				var expected = i + 1;
				Assert.True(results[i].HasValue);
				Assert.Equal(expected, results[i].Value);
			}
		}

		[Fact]
		public void ReadColumn5_ShouldProduceExpectedResults()
		{
			var stripeStreams = GetStripeStreamCollection();
			var longReader = new LongReader(stripeStreams, 5);
			var results = longReader.Read().ToArray();

			Assert.Equal(1920800, results.Length);
			for (int i = 0; i < results.Length; i++)
			{
				var expected = ((i / 70) * 500) % 10000 + 500;
				Assert.True(results[i].HasValue);
				Assert.Equal(expected, results[i].Value);
			}
		}

		[Fact]
		public void ReadColumn7_ShouldProduceExpectedResults()
		{
			var stripeStreams = GetStripeStreamCollection();
			var longReader = new LongReader(stripeStreams, 7);
			var results = longReader.Read().ToArray();

			Assert.Equal(1920800, results.Length);
			for (int i = 0; i < results.Length; i++)
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
			var longReader = new LongReader(stripeStreams, 8);
			var results = longReader.Read().ToArray();

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
			var longReader = new LongReader(stripeStreams, 9);
			var results = longReader.Read().ToArray();

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
