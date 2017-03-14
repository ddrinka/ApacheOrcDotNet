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
    public class StringColumn_Test
    {
		[Fact]
		public void RoundTrip_StringColumn()
		{
			RoundTripSingleValue_Direct(70000);
			RoundTripSingleValue_Dictionary(70000);
		}

		void RoundTripSingleValue_Direct(int numValues)
		{
			var random = new Random(123);
			var pocos = GenerateRandomStrings(random, numValues, numValues).Select(s => new SingleValuePoco { Value = s }).ToList();
			var results = RoundTripSingleValue(pocos);
			for (int i = 0; i < numValues; i++)
				Assert.Equal(pocos[i].Value, results[i]);
		}

		void RoundTripSingleValue_Dictionary(int numValues)
		{
			var random = new Random(123);
			var pocos = GenerateRandomStrings(random, numValues, 100).Select(s => new SingleValuePoco { Value = s }).ToList();
			var results = RoundTripSingleValue(pocos);

			for (int i = 0; i < numValues; i++)
				Assert.Equal(pocos[i].Value, results[i]);
		}

		IEnumerable<string> GenerateRandomStrings(Random rnd, int count, int uniqueCount)
		{
			var strings = new List<string>(uniqueCount);
			for (int i = 0; i < uniqueCount; i++)
				strings.Add(GenerateRandomString(rnd));

			for (int i = 0; i < count; i++)
			{
				var id = rnd.Next() % uniqueCount;
				yield return strings[id];
			}
		}

		string GenerateRandomString(Random rnd)
		{
			var minimumLength = 0;
			var maximumLength = 25;
			var minimumAscii = 0x20;
			var maximumAscii = 0x7e;
			var length = (rnd.Next() % (maximumLength - minimumLength + 1)) + minimumLength;
			var sb = new StringBuilder();
			for (int i = 0; i < length; i++)
				sb.Append((byte)((rnd.Next() % (maximumAscii - minimumAscii + 1)) + minimumAscii));
			return sb.ToString();
		}

		string[] RoundTripSingleValue(IEnumerable<SingleValuePoco> pocos)
		{
			var stream = new MemoryStream();
			Footer footer;
			StripeStreamHelper.Write(stream, pocos, out footer);
			var stripeStreams = StripeStreamHelper.GetStripeStreams(stream, footer);
			var reader = new ApacheOrcDotNet.ColumnTypes.StringReader(stripeStreams, 1);
			return reader.Read().ToArray();
		}

		class SingleValuePoco
		{
			public string Value { get; set; }
		}
    }
}
