using ApacheOrcDotNet.Encodings;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace ApacheOrcDotNet.Test.Encodings
{
    public class IntegerRunLengthEncodingV2_Test
    {
		[Fact]
		public void ReadWrite_ShortRepeat()
		{
			var longs = new long[] { 10000, 10000, 10000, 10000, 10000 };
			var bytes = new byte[] { 0x0a, 0x27, 0x10 };
			TestRead(longs, bytes, false);
			TestWrite(bytes, longs, false, false);
		}

		[Fact]
		public void RoundTrip_ShortRepeat()
		{
			TestRoundTrip(new long[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 }, false, false);
			TestRoundTrip(new long[] { -1, -1, -1, -1, -1, -1, -1, -1, -1, -1 }, true, false);
			TestRoundTrip(new long[] { 0xffff, 0xffff, 0xffff, 0xffff }, false, false);
			TestRoundTrip(new long[] { 0xffffff, 0xffffff, 0xffffff, 0xffffff }, false, false);
			TestRoundTrip(new long[] { 0xffffffff, 0xffffffff, 0xffffffff, 0xffffffff }, false, false);
			TestRoundTrip(new long[] { 0x11223344, 0x11223344, 0x11223344, 0x11223344 }, false, false);
			TestRoundTrip(new long[] { 0x1122334455, 0x1122334455, 0x1122334455, 0x1122334455 }, false, false);
			TestRoundTrip(new long[] { 0x112233445566, 0x112233445566, 0x112233445566, 0x112233445566 }, false, false);
			TestRoundTrip(new long[] { 0x11223344556677, 0x11223344556677, 0x11223344556677, 0x11223344556677 }, false, false);
			TestRoundTrip(new long[] { 0x1122334455667788, 0x1122334455667788, 0x1122334455667788, 0x1122334455667788 }, false, false);
		}

		[Fact]
		public void ReadWrite_Direct()
		{
			var longs = new long[] { 23713, 43806, 57005, 48879 };
			var bytes = new byte[] { 0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef };
			TestRead(longs, bytes, false);
			TestWrite(bytes, longs, false, false);
		}

		[Fact]
		public void RoundTrip_Direct()
		{
			var longs = new long[] { -5, 5, -5, 5 };
			TestRoundTrip(longs, true, false);
			TestRoundTrip(longs, true, true);
		}

		[Fact]
		public void ReadWrite_PatchedBase()
		{
			var longs = new long[] { 2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090, 2100, 2110, 2120, 2130, 2140, 2150, 2160, 2170, 2180, 2190 };
			var bytes = new byte[] { 0x8e, 0x13, 0x2b, 0x21, 0x07, 0xd0, 0x1e, 0x00, 0x14, 0x70, 0x28, 0x32, 0x3c, 0x46, 0x50, 0x5a, 0x64, 0x6e, 0x78, 0x82, 0x8c, 0x96, 0xa0, 0xaa, 0xb4, 0xbe, 0xfc, 0xe8 };
			TestRead(longs, bytes, false);
			TestWrite(bytes, longs, false, false);
		}

		[Fact]
		public void ReadWrite_Delta()
		{
			var longs = new long[] { 2, 3, 5, 7, 11, 13, 17, 19, 23, 29 };
			var bytes = new byte[] { 0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46 };
			TestRead(longs, bytes, false);
			TestWrite(bytes, longs, false, true);
		}

		[Fact]
		public void ReadWrite_Delta2()
		{
			var longs = new long[0x120];
			for (int i = 0; i < longs.Length; i++)
				longs[i] = 0x6;

			var bytes = new byte[] { 0xc1, 0x1f, 0x0c, 0x00 };
			TestRead(longs, bytes, true);
			TestWrite(bytes, longs, true, false);
		}

		[Fact]
		public void RoundTrip_DeltaRepeatingUnsigned()
		{
			var longs = new long[] { 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000 };
			TestRoundTrip(longs, false, true);
			TestRoundTrip(longs, false, false);
		}

		[Fact]
		public void RoundTrip_DeltaRepeatingSignedPositive()
		{
			var longs = new long[] { 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000, 10000 };
			TestRoundTrip(longs, true, true);
			TestRoundTrip(longs, true, false);
		}

		[Fact]
		public void RoundTrip_DeltaRepeatingSignedNegative()
		{
			var longs = new long[] { -10000, -10000, -10000, -10000, -10000, -10000, -10000, -10000, -10000, -10000, -10000, -10000 };
			TestRoundTrip(longs, true, true);
			TestRoundTrip(longs, true, false);
		}

		[Fact]
		public void RoundTrip_DeltaFixedOffsetPositive()
		{
			var longs = new long[] { 10000, 30000, 50000, 70000, 90000, 110000 };
			TestRoundTrip(longs, false, true);
			TestRoundTrip(longs, false, false);
		}

		[Fact]
		public void RoundTrip_DeltaFixedOffsetNegativeBasePositiveDelta()
		{
			var longs = new long[] { -10000, 10000, 30000, 50000, 70000, 90000 };
			TestRoundTrip(longs, true, true);
			TestRoundTrip(longs, true, false);
		}

		[Fact]
		public void RoundTrip_DeltaFixedOffsetNegativeBaseNegativeDelta()
		{
			var longs = new long[] { -10000, -30000, -50000, -70000, -90000, -110000 };
			TestRoundTrip(longs, true, true);
			TestRoundTrip(longs, true, false);
		}

		[Fact]
		public void RoundTrip_DeltaFixedOffsetPositiveBaseNegativeDelta()
		{
			var longs = new long[] { 10000, -10000, -30000, -50000, -70000, -90000 };
			TestRoundTrip(longs, true, true);
			TestRoundTrip(longs, true, false);
		}

		[Fact]
		public void RoundTrip_DeltaVariedOffsetPositive()
		{
			var longs = new long[] { 1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096 };
			TestRoundTrip(longs, false, true);
			TestRoundTrip(longs, false, false);
		}

		[Fact]
		public void RoundTrip_DeltaVariedOffsetNegative()
		{
			var longs = new long[] { -1, -2, -4, -8, -16, -32, -64, -128, -256, -512, -1024, -2048, -4096 };
			TestRoundTrip(longs, true, true);
			TestRoundTrip(longs, true, false);
		}

		#region From Java Source
		[Fact]
		public void RoundTrip_FixedDeltaZero()
		{
			var longs = new List<long>();
			for (int i = 0; i < 5120; i++)
				longs.Add(123);
			TestRoundTrip(longs.ToArray(), false, false, 50);
		}

		[Fact]
		public void RoundTrip_FixedDeltaOne()
		{
			var longs = new List<long>();
			for (int i = 0; i < 5120; i++)
				longs.Add(i % 512);
			TestRoundTrip(longs.ToArray(), false, false, 40);
		}

		[Fact]
		public void RoundTrip_FixedDeltaOneDescending()
		{
			var longs = new List<long>();
			for (int i = 0; i < 5120; i++)
				longs.Add(512 - (i % 512));
			TestRoundTrip(longs.ToArray(), false, false, 50);
		}

		[Fact]
		public void RoundTrip_FixedDeltaLarge()
		{
			var longs = new List<long>();
			for (int i = 0; i < 5120; i++)
				longs.Add(i % 512 + ((i % 512) * 100));
			TestRoundTrip(longs.ToArray(), false, false, 50);
		}

		[Fact]
		public void RoundTrip_FixedDeltaLargeDescending()
		{
			var longs = new List<long>();
			for (int i = 0; i < 5120; i++)
				longs.Add((512 - i % 512) + ((i % 512) * 100));
			TestRoundTrip(longs.ToArray(), false, false, 60);
		}

		[Fact]
		public void RoundTrip_ShortRepeatB()
		{
			var longs = new List<long>();
			for (int i = 0; i < 5; i++)
				longs.Add(10);
			TestRoundTrip(longs.ToArray(), false, false, 2);
		}

		[Fact]
		public void RoundTrip_UnknownSign()
		{
			var longs = new List<long>();
			longs.Add(0);
			for (int i = 0; i < 511; i++)
				longs.Add(i);
			TestRoundTrip(longs.ToArray(), false, false, 642);
		}

		[Fact]
		public void RoundTrip_PatchedBase()
		{
			var longs = new List<long>();
			var random = new Random(123);
			longs.Add(10000000);
			for (int i = 0; i < 511; i++)
				longs.Add(random.Next() % (i + 1));
			TestRoundTrip(longs.ToArray(), false, false, 583);
		}
		#endregion

		void TestRead(long[] expected, byte[] input, bool isSigned)
		{
			var stream = new MemoryStream(input);
			var reader = new IntegerRunLengthEncodingV2Reader(stream, isSigned);
			var actual = reader.Read().ToArray();
			Assert.Equal(expected.Length, actual.Length);
			for (int i = 0; i < expected.Length; i++)
				Assert.Equal(expected[i], actual[i]);
		}

		void TestWrite(byte[] expected, long[] input, bool isSigned, bool aligned)
		{
			var stream = new MemoryStream();
			var writer = new IntegerRunLengthEncodingV2Writer(stream);
			var inputArraySegment = new ArraySegment<long>(input);
			writer.Write(inputArraySegment, isSigned, aligned);
			var actual = stream.ToArray();
			Assert.Equal(expected.Length, actual.Length);
			for (int i = 0; i < expected.Length; i++)
				Assert.Equal(expected[i], actual[i]);
		}

		void TestRoundTrip(long[] test, bool isSigned, bool aligned, int? expectedEncodeLength = null)
		{
			var stream = new MemoryStream();
			var writer = new IntegerRunLengthEncodingV2Writer(stream);
			var arraySegment = new ArraySegment<long>(test);
			writer.Write(arraySegment, isSigned, aligned);

			//If we know the encode length, make sure it's correct
			if (expectedEncodeLength.HasValue)
				Assert.Equal(expectedEncodeLength.Value, stream.Length);

			stream.Seek(0, SeekOrigin.Begin);

			var reader = new IntegerRunLengthEncodingV2Reader(stream, isSigned);
			var result = reader.Read().ToArray();

			//Make sure all bytes in the written stream were consumed
			Assert.Equal(stream.Length, stream.Position - 1);

			//Check the actual values
			Assert.Equal(test.Length, result.Length);
			for (int i = 0; i < test.Length; i++)
				Assert.Equal(test[i], result[i]);
		}
    }
}
