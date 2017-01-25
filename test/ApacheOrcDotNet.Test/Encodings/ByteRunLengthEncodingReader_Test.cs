using ApacheOrcDotNet.Encodings;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace ApacheOrcDotNet.Test.Encodings
{
    public class ByteRunLengthEncodingReader_Test
    {
		[Fact]
		public void Read_Repeated()
		{
			var expected = new byte[100];
			var input = new byte[] { 0x61, 0x00 };
			CheckRead(expected, input);
		}

		[Fact]
		public void Read_Literals()
		{
			var expected = new byte[] { 0x44, 0x45 };
			var input = new byte[] { 0xfe, 0x44, 0x45 };
			CheckRead(expected, input);
		}

		void CheckRead(byte[] expected, byte[] input)
		{
			var stream = new MemoryStream(input);
			var reader = new ByteRunLengthEncodingReader(stream);
			var actual = reader.Read().ToArray();
			Assert.Equal(expected.Length, actual.Length);
			for (int i = 0; i < expected.Length; i++)
				Assert.Equal(expected[i], actual[i]);
		}
    }
}
