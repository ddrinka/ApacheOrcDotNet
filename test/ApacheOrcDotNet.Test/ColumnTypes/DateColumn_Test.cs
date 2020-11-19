using ApacheOrcDotNet.ColumnTypes;
using ApacheOrcDotNet.FluentSerialization;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace ApacheOrcDotNet.Test.ColumnTypes {
    public class DateColumn_Test {
        [Fact]
        public void RoundTrip_DateColumn() {
            RoundTripSingleValue(70000);
        }

        void RoundTripSingleValue(int numValues) {
            var random = new Random(123);
            var pocos = GenerateRandomDates(random, numValues).Select(t => new SingleValuePoco { Value = t }).ToList();

            var configuration = new SerializationConfiguration()
                                .ConfigureType<SingleValuePoco>()
                                    .ConfigureProperty(x => x.Value, x => x.SerializeAsDate = true)
                                    .Build();

            var stream = new MemoryStream();
            Footer footer;
            StripeStreamHelper.Write(stream, pocos, out footer, configuration);
            var stripeStreams = StripeStreamHelper.GetStripeStreams(stream, footer);
            var reader = new DateReader(stripeStreams, 1);
            var results = reader.Read().ToArray();

            for (int i = 0; i < numValues; i++)
                Assert.Equal(pocos[i].Value, results[i]);
        }

        IEnumerable<DateTime> GenerateRandomDates(Random rnd, int count) {
            var baseTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            for (int i = 0; i < count; i++) {
                yield return baseTime.AddDays((rnd.Next() % (360 * 200)) - (360 * 100));
            }
        }

        class SingleValuePoco {
            public DateTime Value { get; set; }
        }
    }
}
