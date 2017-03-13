using ApacheOrcDotNet.Encodings;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using Xunit;

namespace ApacheOrcDotNet.Test.Encodings
{
    public class BitManipulation_Test
    {
		[Fact]
		public void CheckedReadByte_DataIsAvailable_ShouldReturnAByte()
		{
			var data = new byte[] { 0x11 };
			var stream = new MemoryStream(data);
			var result = stream.CheckedReadByte();
			Assert.Equal(0x11, result);
		}

		[Fact]
		public void CheckedReadByte_DataNotAvailable_ShouldThrow()
		{
			var data = new byte[] { 0x11 };
			var stream = new MemoryStream(data);
			var read1 = stream.CheckedReadByte();
			try
			{
				var read2 = stream.CheckedReadByte();
				Assert.True(false);
			}
			catch(InvalidOperationException)
			{ }
		}

		[Fact]
		public void ReadLongBE_VariousByteLengthsShouldWork()
		{
			var data = new Dictionary<long, byte[]>
			{
				{0x11 , new byte[] {0x11} },
				{0x1122, new byte[] {0x11, 0x22} },
				{0x112233, new byte[] {0x11, 0x22, 0x33 } },
				{0x11223344, new byte[] { 0x11, 0x22, 0x33, 0x44 } },
				{0x1122334455, new byte[] {0x11, 0x22, 0x33, 0x44, 0x55 } },
				{0x112233445566, new byte[] {0x11, 0x22, 0x33, 0x44, 0x55, 0x66 } },
				{0x11223344556677, new byte[] {0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77 } },
				{0x1122334455667788, new byte[] {0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88 } }
			};

			foreach(var keyval in data)
			{
				var stream = new MemoryStream(keyval.Value);
				long expected = keyval.Key;
				var actual = stream.ReadLongBE(keyval.Value.Length);
				Assert.Equal(expected, actual);
			}
		}

		[Fact]
		public void ZigzagDecode_ShouldZigZag()
		{
			var data = new Dictionary<long, long>
			{
				{0 , 0},
				{-1, 1},
				{1 , 2},
				{-2, 3},
				{2 , 4}
			};

			foreach(var keyval in data)
			{
				var expected = keyval.Key;
				var actual = keyval.Value.ZigzagDecode();
				Assert.Equal(expected, actual);
			}
		}

		[Fact]
		public void ZigzagEncode_ShouldRoundTrip()
		{
			var data = new List<long> { 0, 1, 2, 3, 4 };
			foreach(var value in data)
			{
				var expected = value;
				var encoded = value.ZigzagEncode();
				var actual = encoded.ZigzagDecode();
				Assert.Equal(expected, actual);
			}
		}

		[Fact]
		public void ReadBitpackedIntegers_VariousBitWidthsShouldWork()
		{
			CheckBitpackedIntegersFromString(
				new long[] { 1, 0, 1, 1, 0 },
				"1 0 1 1 0",
				1);
	
			CheckBitpackedIntegersFromString(
				new long[] { 0, 1, 2, 3, 0, 1, 2, 3 },
				"00 01 10 11 00 01 10 11",
				2);

			CheckBitpackedIntegersFromString(
				new long[] {0, 1, 2, 3, 4, 5, 6, 7},
				"000 001 010 011 100 101 110 111",
				3);

			CheckBitpackedIntegersFromString(
				new long[] {0, 1, 2, 4, 8, 15},
				"0000 0001 0010 0100 1000 1111",
				4);

			CheckBitpackedIntegersFromString(
				new long[] {0, 1, 2, 4, 8, 16, 31},
				"00000 00001 00010 00100 01000 10000 11111",
				5);

			CheckBitpackedIntegersFromString(
				new long[] { 0, 1, 2, 4, 8, 16, 32, 63 },
				"000000 000001 000010 000100 001000 010000 100000 111111",
				6);

			CheckBitpackedIntegersFromString(
				new long[] { 0, 1, 2, 4, 8, 16, 32, 64, 127 },
				"0000000 0000001 0000010 0000100 0001000 0010000 0100000 1000000 1111111",
				7);

			CheckBitpackedIntegersFromString(
				new long[] { 0, 1, 2, 4, 8, 16, 32, 64, 128, 255 },
				"00000000 00000001 00000010 00000100 00001000 00010000 00100000 01000000 10000000 11111111",
				8);

			CheckBitpackedIntegersFromString(
				new long[] { 0, 511, 0 },
				"000000000 111111111 000000000",
				9);

			CheckBitpackedIntegersFromString(
				new long[] { 0, 1023, 0 },
				"0000000000 1111111111 0000000000",
				10);

			CheckBitpackedIntegersFromString(
				new long[] { 0, 131071, 0 },
				"00000000000000000 11111111111111111 00000000000000000",
				17);

			CheckBitpackedIntegersFromString(
				new long[] { 0, 8589934591, 0 },
				"000000000000000000000000000000000 111111111111111111111111111111111 000000000000000000000000000000000",
				33);

			CheckBitpackedIntegersFromString(
				new long[] { 0, -1, 0 },
				"0000000000000000000000000000000000000000000000000000000000000000 " +
				"1111111111111111111111111111111111111111111111111111111111111111 " +
				"0000000000000000000000000000000000000000000000000000000000000000",
				64);
		}

		void CheckBitpackedIntegersFromString(long[] expected, string bits, int bitWidth)
		{
			var bytesExpected = BitStringToByteArray(bits);
			var readStream = new MemoryStream(bytesExpected);
			var readActual=readStream.ReadBitpackedIntegers(bitWidth, expected.Length).ToArray();
			Assert.Equal(expected.Length, readActual.Length);
			for (int i = 0; i < expected.Length; i++)
				Assert.Equal(expected[i], readActual[i]);

			var writeStream = new MemoryStream();
			writeStream.WriteBitpackedIntegers(expected, bitWidth);
			var writeBytesActual = writeStream.ToArray();
			Assert.Equal(bytesExpected.Length, writeBytesActual.Length);
			for (int i = 0; i < bytesExpected.Length; i++)
				Assert.Equal(bytesExpected[i], writeBytesActual[i]);
		}

		byte[] BitStringToByteArray(string bits)
		{
			var result = new List<byte>();
			int bitCount = 0;
			byte currentByte = 0;
			foreach(char c in bits)
			{
				if (c != '1' && c != '0')
					continue;
				currentByte <<= 1;
				if (c == '1')
					currentByte |= 1;
				if (++bitCount > 7)
				{
					result.Add(currentByte);
					bitCount = 0;
				}
			}
			if (bitCount > 0)
			{
				currentByte <<= (8 - bitCount);		//Shift in zeros to fill out the rest of this byte
				result.Add(currentByte);
			}

			return result.ToArray();
		}

		[Fact]
		public void ReadVarIntSigned_ShouldMatchExpected()
		{
			var data = new Dictionary<long, byte[]>
			{
				{    0, new byte[] {0x00 } },
				{    1, new byte[] {0x01 } },
				{  127, new byte[] {0x7f } },
				{  128, new byte[] {0x80,0x01} },
				{  129, new byte[] {0x81, 0x01 } },
				{16383, new byte[] {0xff, 0x7f } },
				{16384, new byte[] {0x80, 0x80, 0x01 } },
				{16385, new byte[] {0x81, 0x80, 0x01 } }
			};

			foreach(var keyval in data)
			{
				var stream = new MemoryStream(keyval.Value);
				long expected = keyval.Key;
				var actual = stream.ReadVarIntUnsigned();
				Assert.Equal(expected, actual);
			}
		}

		[Fact]
		public void RoundTrip_VarInt_Signed()
		{
			var longs = new long[] { 0, 1000, -1000, 10000, -10000, 100000, -100000, Int32.MaxValue, Int32.MinValue };
			foreach(var expected in longs)
			{
				using (var stream = new MemoryStream())
				{
					stream.WriteVarIntSigned(expected);
					stream.Seek(0, SeekOrigin.Begin);
					var actual = stream.ReadVarIntSigned();
					Assert.Equal(expected, actual);
				}
			}
		}

		[Fact]
		public void RoundTrip_VarInt_SignedExtents()
		{
			var longs = new long[] { Int64.MaxValue, Int64.MinValue };
			foreach (var expected in longs)
			{
				using (var stream = new MemoryStream())
				{
					stream.WriteVarIntSigned(expected);
					stream.Seek(0, SeekOrigin.Begin);
					var actual = stream.ReadVarIntSigned();
					Assert.Equal(expected, actual);
				}
			}
		}

		[Fact]
		public void RoundTrip_VarInt_Unsigned()
		{
			var longs = new long[] { 0, 1000, 10000, 100000, UInt32.MaxValue };
			foreach (var expected in longs)
			{
				using (var stream = new MemoryStream())
				{
					stream.WriteVarIntUnsigned(expected);
					stream.Seek(0, SeekOrigin.Begin);
					var actual = stream.ReadVarIntUnsigned();
					Assert.Equal(expected, actual);
				}
			}
		}

		[Fact]
		public void RoundTrip_VarInt_UnsignedExtents()
		{
			using (var stream = new MemoryStream())
			{
				ulong expected = 0xffffffffffffffff;
				stream.WriteVarIntUnsigned((long)expected);
				stream.Seek(0, SeekOrigin.Begin);
				var actual = stream.ReadVarIntUnsigned();
				Assert.Equal(expected, (ulong)actual);
			}
		}

		[Fact]
		public void RoundTrip_VarInt_BigInt_DebuggingSequences()
		{
			var sequences = new List<Tuple<uint, uint, uint, bool>>
			{
				Tuple.Create(0xffffffffu, 0xffffffffu, 0xffffffffu, false),
				Tuple.Create(0xffffffffu, 0xffffffffu, 0xffffffffu, true),
				Tuple.Create(0xf80fe03fu, 0x3f80fe03u, 0x3f80fe0u, false),
				Tuple.Create(0xf80fe03fu, 0x3f80fe03u, 0x3f80fe0u, true),
				Tuple.Create(~0xf80fe03fu, ~0x3f80fe03u, ~0x3f80fe0u, false),
				Tuple.Create(~0xf80fe03fu, ~0x3f80fe03u, ~0x3f80fe0u, true),
				Tuple.Create(0x870e1c38u, 0x3870e1c3u, 0xc3870e1cu, true),
				Tuple.Create(0x870e1c38u, 0x3870e1c3u, 0xc3870e1cu, false),
				Tuple.Create(~0x870e1c38u, ~0x3870e1c3u, ~0xc3870e1cu, true),
				Tuple.Create(~0x870e1c38u, ~0x3870e1c3u, ~0xc3870e1cu, false),
			};

			CheckBigIntVarInt(sequences);
		}

		[Fact]
		public void RoundTrip_VarInt_BigInt_Random()
		{
			var values = new List<Tuple<uint, uint, uint, bool>>();

			var random = new Random(123);
			for (int i = 0; i < 1000; i++)
			{
				var buffer = new byte[4 * 3 + 1];
				random.NextBytes(buffer);

				values.Add(Tuple.Create(BitConverter.ToUInt32(buffer, 8), BitConverter.ToUInt32(buffer, 4), BitConverter.ToUInt32(buffer, 0), buffer[12] % 2 == 0));
			}

			CheckBigIntVarInt(values);
		}

		[Fact]
		public void RoundTrip_VarInt_BigInt_Debugging()
		{
			var values = new[] { Tuple.Create(0xffffffffu, 0xffffffffu, 0xffffffffu, true) };
			CheckBigIntVarInt(values);
		}

		void CheckBigIntVarInt(IEnumerable<Tuple<uint,uint,uint,bool>> values)
		{
			foreach (var tuple in values)
			{
				var low = tuple.Item3;
				var mid = tuple.Item2;
				var high = tuple.Item1;
				var isNegative = tuple.Item4;

				var expected = new BigInteger(low) | (new BigInteger(mid) << 32) | (new BigInteger(high) << 64);
				if (isNegative)
					expected = -(expected + 1);

				using (var stream = new MemoryStream())
				{
					stream.WriteVarIntSigned(low, mid, high, isNegative);
					stream.Seek(0, SeekOrigin.Begin);
					var actual = stream.ReadBigVarInt();
					Assert.Equal(expected, actual);
				}
			}
		}
	}
}
