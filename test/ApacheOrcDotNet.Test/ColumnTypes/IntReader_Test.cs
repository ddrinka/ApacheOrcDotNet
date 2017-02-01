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
    public class IntReader_Test
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
			var intReader = new IntReader(stripeStreams, 1);
			var results = intReader.Read().ToArray();

			Assert.Equal(1920800, results.Length);
			for(int i=0;i<results.Length;i++)
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
			var intReader = new IntReader(stripeStreams, 5);
			var results = intReader.Read().ToArray();

			Assert.Equal(1920800, results.Length);
			for (int i = 0; i < results.Length; i++)
			{
				var expected = ((i / 70) * 500) % 10000 + 500;
				Assert.True(results[i].HasValue);
				Assert.Equal(expected, results[i].Value);
			}
		}
	}
}
