using ApacheOrcDotNet.Encodings;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace ApacheOrcDotNet.Test.Encodings
{
    public class BitEncoding_Test
    {
		[Fact]
		public void ReadWrite()
		{
			var bools = new bool[] { true, false, false, false, false, false, false, false };
			var bytes = new byte[] { 0xff, 0x80 };
			TestRead(bools, bytes);
			TestWrite(bytes, bools);
		}

		[Fact]
		public void RoundTrip_1()
		{
			var bools0 = new bool[] { false };
			var bools1 = new bool[] { true };

			TestRoundTrip(bools0);
			TestRoundTrip(bools1);
		}

		[Fact]
		public void RoundTrip_2()
		{
			var bools0 = new bool[] { false, false };
			var bools1 = new bool[] { false, true };
			var bools2 = new bool[] { true, false };
			var bools3 = new bool[] { true, true };

			TestRoundTrip(bools0);
			TestRoundTrip(bools1);
			TestRoundTrip(bools2);
			TestRoundTrip(bools3);
		}

		[Fact]
		public void RoundTrip_Random()
		{
			var bools = new List<bool>();
			var random = new Random(123);
			for(int i=0;i<10000;i++)
			{
				bools.Add((random.Next() & 1) == 0);
			}

			TestRoundTrip(bools.ToArray());
		}

		void TestRead(bool[] expected, byte[] input)
		{
			var stream = new MemoryStream(input);
			var reader = new BitReader(stream);
			var actual = reader.Read().ToArray();
			Assert.Equal(expected.Length, actual.Length);
			for (int i = 0; i < expected.Length; i++)
				Assert.Equal(expected[i], actual[i]);
		}

		void TestWrite(byte[] expected, bool[] input)
		{
			var stream = new MemoryStream();
			var writer = new BitWriter(stream);
			writer.Write(input);
			var actual = stream.ToArray();
			Assert.Equal(expected.Length, actual.Length);
			for (int i = 0; i < expected.Length; i++)
				Assert.Equal(expected[i], actual[i]);
		}

		void TestRoundTrip(bool[] values, int? expectedEncodeLength = null)
		{
			var stream = new MemoryStream();
			var writer = new BitWriter(stream);
			writer.Write(values);

			//If we know the encode length, make sure it's correct
			if (expectedEncodeLength.HasValue)
				Assert.Equal(expectedEncodeLength.Value, stream.Length);

			stream.Seek(0, SeekOrigin.Begin);

			var reader = new BitReader(stream);
			var result = reader.Read().ToArray();

			//Make sure all bytes in the written stream were consumed
			Assert.Equal(stream.Length, stream.Position);

			//Check the actual values
			Assert.InRange(result.Length, values.Length, values.Length + 7);		//We may end up with up to 7 extra bits--ignore these
			for (int i = 0; i < values.Length; i++)
				Assert.Equal(values[i], result[i]);
		}
	}
}
