using ApacheOrcDotNet.Encodings;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace ApacheOrcDotNet.Test.Encodings
{
    public class ByteRunLengthEncoding_Test
    {
		[Fact]
		public void ReadWrite_Repeated()
		{
			var values = new byte[100];
			var encoded = new byte[] { 0x61, 0x00 };
			TestRead(values, encoded);
			TestWrite(encoded, values);
		}

		[Fact]
		public void ReadWrite_Literals()
		{
			var values = new byte[] { 0x44, 0x45 };
			var encoded = new byte[] { 0xfe, 0x44, 0x45 };
			TestRead(values, encoded);
			TestWrite(encoded, values);
		}

		[Fact]
		public void ReadWrite_SetOfRepeats()
		{
			var values = new byte[] { 0x1, 0x1, 0x1, 0x2, 0x2, 0x2, 0x3, 0x3, 0x3 };
			var encoded = new byte[] { 0x0, 0x1, 0x0, 0x2, 0x0, 0x3 };
			TestRead(values, encoded);
			TestWrite(encoded, values);
		}

		[Fact]
		public void ReadWrite_Repeats_Literal_Repeats()
		{
			var values = new byte[] { 0x1, 0x1, 0x1, 0x2, 0x3, 0x4, 0x5, 0x5, 0x5 };
			var encoded = new byte[] { 0x0, 0x1, 0xfd, 0x2, 0x3, 0x4, 0x0, 0x5 };
			TestRead(values, encoded);
			TestWrite(encoded, values);
		}

		[Fact]
		public void RoundTrip_InterspersedRepeats()
		{
			var values = new byte[] { 0x1, 0x2, 0x2, 0x2, 0x3, 0x3, 0x4, 0x4, 0x4, 0x5 };
			TestRoundTrip(values, 2 + 2 + 3 + 2 + 2);
		}

		[Fact]
		public void RoundTrip_130Repeats()
		{
			var values = new List<byte>();
			for (int i = 0; i < 5; i++)
				for (int j = 0; j < 130; j++)
					values.Add(0x1);
			TestRoundTrip(values.ToArray(), 5 * 2);
		}

		[Fact]
		public void RoundTrip_128Literals()
		{
			var values = new List<byte>();
			for (int i = 0; i < 5; i++)
				for (int j = 0; j < 128; j++)
					values.Add((byte)j);
			TestRoundTrip(values.ToArray(), 5 * (128 + 1));
		}

		void TestRead(byte[] expected, byte[] input)
		{
			var stream = new MemoryStream(input);
			var reader = new ByteRunLengthEncodingReader(stream);
			var actual = reader.Read().ToArray();
			Assert.Equal(expected.Length, actual.Length);
			for (int i = 0; i < expected.Length; i++)
				Assert.Equal(expected[i], actual[i]);
		}

		void TestWrite(byte[] expected, byte[] input)
		{
			var stream = new MemoryStream();
			var writer = new ByteRunLengthEncodingWriter(stream);
			writer.Write(input);
			var actual = stream.ToArray();
			Assert.Equal(expected.Length, actual.Length);
			for (int i = 0; i < expected.Length; i++)
				Assert.Equal(expected[i], actual[i]);
		}
		
		void TestRoundTrip(byte[] values, int? expectedEncodeLength = null)
		{
			var stream = new MemoryStream();
			var writer = new ByteRunLengthEncodingWriter(stream);
			writer.Write(values);

			//If we know the encode length, make sure it's correct
			if (expectedEncodeLength.HasValue)
				Assert.Equal(expectedEncodeLength.Value, stream.Length);

			stream.Seek(0, SeekOrigin.Begin);

			var reader = new ByteRunLengthEncodingReader(stream);
			var result = reader.Read().ToArray();

			//Make sure all bytes in the written stream were consumed
			Assert.Equal(stream.Length, stream.Position);

			//Check the actual values
			Assert.Equal(values.Length, result.Length);
			for (int i = 0; i < values.Length; i++)
				Assert.Equal(values[i], result[i]);
		}
	}
}
