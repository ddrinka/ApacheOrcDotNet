using ApacheOrcDotNet.Encodings;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace ApacheOrcDotNet.Test.Encodings
{
    public class BitReader_Test
    {
		[Fact]
		public void Read()
		{
			var expected = new bool[] { true, false, false, false, false, false, false, false };
			var input = new byte[] { 0xff, 0x80 };
			TestRead(expected, input);
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
    }
}
