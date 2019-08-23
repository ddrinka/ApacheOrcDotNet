using ApacheOrcDotNet.ColumnTypes;
using ApacheOrcDotNet.FluentSerialization;
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


		[Fact]
		public void RoundTrip_DecimalAllNulls()
		{
			RoundTripNulls(70000);
		}

        [Fact]
        public void TooMuchPrecision_Throws()
        {
            var serializationConfiguration = new SerializationConfiguration()
                .ConfigureType<SingleValuePoco>()
                    .ConfigureProperty(x => x.Value, x => { x.DecimalPrecision = 14; x.DecimalScale = 9; })
                    .Build();

            var stream = new MemoryStream();
            var goodWriter = new OrcWriter<SingleValuePoco>(stream, new WriterConfiguration(), serializationConfiguration);
            goodWriter.AddRow(new SingleValuePoco { Value = 12345.678901234m });
            goodWriter.Dispose();

            var badWriter = new OrcWriter<SingleValuePoco>(stream, new WriterConfiguration(), serializationConfiguration);
            badWriter.AddRow(new SingleValuePoco { Value = 123456.789012345m });
            Assert.Throws<OverflowException>(() =>
            {
                badWriter.Dispose();
            });
        }

		void RoundTripSingleValue(int numValues)
		{
			var pocos = new List<SingleValuePoco>();
			var random = new Random(123);
            for (int i = 0; i < numValues; i++)
            {
                decimal wholePortion = random.Next() % 99999;       //14-9
                decimal decimalPortion = random.Next() % 999999999; //9
                decimal value = wholePortion + decimalPortion / 1000000000m;
                pocos.Add(new SingleValuePoco { Value = value });
            }

			var configuration = new SerializationConfiguration()
					.ConfigureType<SingleValuePoco>()
						.ConfigureProperty(x => x.Value, x => { x.DecimalPrecision = 14; x.DecimalScale = 9; })
						.Build();

			var stream = new MemoryStream();
			Footer footer;
			StripeStreamHelper.Write(stream, pocos, out footer, configuration);
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

		void RoundTripNulls(int numValues)
		{
			var pocos = new List<NullableSingleValuePoco>();
			for (int i = 0; i < numValues; i++)
				pocos.Add(new NullableSingleValuePoco());

			var stream = new MemoryStream();
			Footer footer;
			StripeStreamHelper.Write(stream, pocos, out footer);
			var stripeStreams = StripeStreamHelper.GetStripeStreams(stream, footer);
			var reader = new DecimalReader(stripeStreams, 1);
			var results = reader.Read().ToArray();

			for (int i = 0; i < numValues; i++)
				Assert.Equal(pocos[i].Value, results[i]);
		}

		class NullableSingleValuePoco
		{
			public decimal? Value { get; set; }
		}
    }
}
