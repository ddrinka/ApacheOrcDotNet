using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Xunit;

namespace ApacheOrcDotNet.Test {
    public class RoundTrip_Test
    {
		[Fact]
		public void SingleStripe_RoundTrip()
		{
			var testElements = new List<RoundTripTestObject>();
			var random = new Random(123);
			for (int i = 0; i < 10000; i++)
				testElements.Add(RoundTripTestObject.Random(random));
			TestRoundTrip(testElements);
		}


        [Fact]
        public void MultipleStripe_RoundTrip()
        {
            var testElements = new List<RoundTripTestObject>();
            var random = new Random(123);
            for (int i = 0; i < 2000000; i++)
                testElements.Add(RoundTripTestObject.Random(random));
            TestRoundTrip(testElements);
        }

        void TestRoundTrip<T>(List<T> expected) where T : new()
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

	class RoundTripTestObject
	{
        readonly static DateTime _dateBase = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

		public int Int { get; set; }
        public long Long { get; set; }
        public short Short { get; set; }
        public uint UInt { get; set; }
        public ulong ULong { get; set; }
        public ushort UShort { get; set; }
        public int? NullableInt { get; set; }
        public byte Byte { get; set; }
        public sbyte SByte { get; set; }
        public bool Bool { get; set; }
        public float Float { get; set; }
        public double Double { get; set; }
        public byte[] ByteArray { get; set; }
        public decimal Decimal { get; set; }
        public DateTime DateTime { get; set; }
        public string String { get; set; }


        public static RoundTripTestObject Random(Random r)
        {
            var result = new RoundTripTestObject
            {
                Int = r.Next(),
                Long = r.Next() * 0xFFFFFFL,
                Short = (short)r.Next(),
                UInt = (uint)r.Next(),
                ULong = (ulong)r.Next() * 0xFFFFFFL,
                UShort = (ushort)r.Next(),
                NullableInt = r.Next() % 10 == 0 ? null : (int?)r.Next(),
                Byte = (byte)r.Next(),
                SByte = (sbyte)r.Next(),
                Bool = r.Next() % 2 == 0,
                Float = (float)r.NextDouble(),
                Double = r.NextDouble(),
                ByteArray = new byte[10],
                Decimal = r.Next() / 1000m,
                DateTime = _dateBase.AddSeconds(r.Next()),
                String = r.Next().ToString()
            };

            r.NextBytes(result.ByteArray);

            return result;
        }

        public override bool Equals(object obj)
        {
            var test = obj as RoundTripTestObject;
            return test != null &&
                   Int == test.Int &&
                   Long == test.Long &&
                   Short == test.Short &&
                   UInt == test.UInt &&
                   ULong == test.ULong &&
                   UShort == test.UShort &&
                   NullableInt == test.NullableInt &&
                   Byte == test.Byte &&
                   SByte == test.SByte &&
                   Bool == test.Bool &&
                   Float == test.Float &&
                   Double == test.Double &&
                   ByteArray.SequenceEqual(test.ByteArray) &&
                   Decimal == test.Decimal &&
                   DateTime == test.DateTime &&
                   String == test.String;
        }

        public override int GetHashCode()
        {
            var hashCode = 291051517;
            hashCode = hashCode * -1521134295 + Int.GetHashCode();
            hashCode = hashCode * -1521134295 + Long.GetHashCode();
            hashCode = hashCode * -1521134295 + Short.GetHashCode();
            hashCode = hashCode * -1521134295 + UInt.GetHashCode();
            hashCode = hashCode * -1521134295 + ULong.GetHashCode();
            hashCode = hashCode * -1521134295 + UShort.GetHashCode();
            hashCode = hashCode * -1521134295 + EqualityComparer<int?>.Default.GetHashCode(NullableInt);
            hashCode = hashCode * -1521134295 + Byte.GetHashCode();
            hashCode = hashCode * -1521134295 + SByte.GetHashCode();
            hashCode = hashCode * -1521134295 + Bool.GetHashCode();
            hashCode = hashCode * -1521134295 + Float.GetHashCode();
            hashCode = hashCode * -1521134295 + Double.GetHashCode();
            hashCode = hashCode * -1521134295 + EqualityComparer<byte[]>.Default.GetHashCode(ByteArray);
            hashCode = hashCode * -1521134295 + Decimal.GetHashCode();
            hashCode = hashCode * -1521134295 + DateTime.GetHashCode();
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(String);
            return hashCode;
        }
    }
}
