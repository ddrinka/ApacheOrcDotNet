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
    public class DecimalReader_Test
    {
		StripeStreamReaderCollection GetStripeStreamCollection()
		{
			var dataFile = new DataFileHelper("decimal.orc");
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
			var decimalReader = new DecimalReader(stripeStreams, 1);
			var results = decimalReader.Read().ToArray();

			Assert.Equal(6000, results.Length);
			for (int i = 0; i < results.Length; i++)
			{
				decimal? expected;
				if(i<2000)
				{
					var decimalPortion= 5 + i;
					var wholePortion = -1000 + i;
					expected = decimal.Parse($"{wholePortion}.{decimalPortion}");
				}
				else if(i<4000)
				{
					expected = null;
				}
				else
				{
					var decimalPortion = (i - 4000) + 1;
					var wholePortion = (i - 4000);
					expected = decimal.Parse($"{wholePortion}.{decimalPortion}");
				}
				Assert.Equal(expected, results[i]);
			}
		}

	}
}
