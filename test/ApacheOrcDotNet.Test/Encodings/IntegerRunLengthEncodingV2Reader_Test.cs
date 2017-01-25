using ApacheOrcDotNet.Encodings;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace ApacheOrcDotNet.Test.Encodings
{
    public class IntegerRunLengthEncodingV2Reader_Test
    {
		[Fact]
		public void Read_ShortRepeat()
		{
			var expected = new long[] { 10000, 10000, 10000, 10000, 10000 };
			var input = new byte[] { 0x0a, 0x27, 0x10 };
			TestRead(expected, input, false);
		}

		[Fact]
		public void Read_Direct()
		{
			var expected = new long[] { 23713, 43806, 57005, 48879 };
			var input = new byte[] { 0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef };
			TestRead(expected, input, false);
		}

		[Fact]
		public void Read_PatchedBase()
		{
			var expected = new long[] { 2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090 };
			var input = new byte[] { 0x8e, 0x09, 0x2b, 0x21, 0x07, 0xd0, 0x1e, 0x00, 0x14, 0x70, 0x28, 0x32, 0x3c, 0x46, 0x50, 0x5a, 0xfc, 0xe8 };
			TestRead(expected, input, false);
		}

		[Fact]
		public void Read_Delta()
		{
			var expected = new long[] { 2, 3, 5, 7, 11, 13, 17, 19, 23, 29 };
			var input = new byte[] { 0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46 };
			TestRead(expected, input, false);
		}

		void TestRead(long[] expected, byte[] input, bool isSigned)
		{
			var stream = new MemoryStream(input);
			var reader = new IntegerRunLengthEncodingV2Reader(stream, isSigned);
			var actual = reader.Read().ToArray();
			Assert.Equal(expected.Length, actual.Length);
			for (int i = 0; i < expected.Length; i++)
				Assert.Equal(expected[i], actual[i]);
		}
    }
}
