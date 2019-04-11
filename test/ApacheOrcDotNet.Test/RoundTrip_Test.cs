using ApacheOrcDotNet.ColumnTypes;
using ApacheOrcDotNet.Test.TestHelpers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace ApacheOrcDotNet.Test
{
    public class RoundTrip_Test
    {
		[Fact]
		public void CompleteStream_IntColumn_SingleStripe_RoundTrip()
		{
			var testElements = new List<IntColumnTest>();
			var random = new Random(123);
			for (int i = 0; i < 70000; i++)
				testElements.Add(IntColumnTest.Random(random));
			TestRoundTripLongColumn(testElements);
		}


        [Fact]
        public void CompleteStream_IntColumn_MultipleStripe_RoundTrip()
        {
            var testElements = new List<IntColumnTest>();
            var random = new Random(123);
            for (int i = 0; i < 32000000; i++)
                testElements.Add(IntColumnTest.Random(random));
            TestRoundTripLongColumn(testElements);
        }

        void TestRoundTripLongColumn<T>(List<T> expected) where T : new()
        {
            var memStream = new MemoryStream();
            using (var writer = new OrcWriter<T>(memStream, new WriterConfiguration())) //Use the default configuration
            {
                writer.AddRows(expected);
            }

            memStream.Seek(0, SeekOrigin.Begin);

            var reader = new OrcReader<T>(memStream);
            var actual = reader.Read().ToList();

            Assert.Equal(expected.Count, actual.Count);
            for (int i = 0; i < expected.Count; i++)
                Assert.Equal(expected[i], actual[i]);
        }
	}

	class IntColumnTest
	{
		public int Column1 { get; set; }
        public long Column2 { get; set; }

        public override bool Equals(object obj)
        {
            if (!(obj is IntColumnTest other))
                return false;

            return other.Column1 == Column1
                && other.Column2 == Column2;
        }

        public static IntColumnTest Random(Random r)
        {
            return new IntColumnTest
            {
                Column1 = r.Next(),
                Column2 = r.Next() * 0xFFFFFFL
            };
        }

        public override int GetHashCode()
        {
            var hashCode = 681758273;
            hashCode = hashCode * -1521134295 + Column1.GetHashCode();
            hashCode = hashCode * -1521134295 + Column2.GetHashCode();
            return hashCode;
        }
    }
}
