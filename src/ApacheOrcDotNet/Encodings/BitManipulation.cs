using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Encodings
{
	public static class BitManipulation
	{
		public static byte CheckedReadByte(this Stream stream)
		{
			var result = stream.ReadByte();
			if (result < 0)
				throw new InvalidOperationException("Read past end of stream");
			return (byte)result;
		}

		public static long ReadLongBE(this Stream stream, int numBytes)
		{
			long result = 0;
			for (int i = numBytes - 1; i >= 0; i--)
			{
				long nextByte = stream.CheckedReadByte();
				result |= (nextByte << (i * 8));
			}
			return result;
		}

		public static long ZigzagDecode(this long value)
		{
			return (long)(((ulong)value) >> 1) ^ -(value & 1);
		}

		public static long ZigzagEncode(this long value)
		{
			return (value << 1) ^ (value >> 63);
		}

		public static int DecodeDirectWidth(this int encodedWidth)
		{
			if (encodedWidth >= 0 && encodedWidth <= 23)
				return encodedWidth + 1;
			switch (encodedWidth)
			{
				case 24: return 26;
				case 25: return 28;
				case 26: return 30;
				case 27: return 32;
				case 28: return 40;
				case 29: return 48;
				case 30: return 56;
				case 31: return 64;
			}
			throw new NotImplementedException($"Unimplemented {nameof(encodedWidth)} {encodedWidth}");
		}

		public static int NumBits(ulong value)
		{
			int result = 0;
			while(value!=0)
			{
				result++;
				value >>= 1;
			}
			return result;
		}

		public static int FindNearestDirectWidth(int approxBits)
		{
			if (approxBits == 0)
				return 1;
			else if (approxBits >= 1 && approxBits <= 24)
				return approxBits;
			else if (approxBits <= 26)
				return 26;
			else if (approxBits <= 28)
				return 28;
			else if (approxBits <= 30)
				return 30;
			else if (approxBits <= 32)
				return 32;
			else if (approxBits <= 40)
				return 40;
			else if (approxBits <= 48)
				return 48;
			else if (approxBits <= 56)
				return 56;
			else
				return 64;
		}

		public static IEnumerable<long> ReadBitpackedIntegers(this Stream stream, int bitWidth, int count)
		{
			byte currentByte = 0;
			int bitsAvailable = 0;
			for (int i = 0; i < count; i++)
			{
				ulong result = 0;
				int neededBits = bitWidth;
				while (neededBits > bitsAvailable)
				{
					result <<= bitsAvailable;   //Make space for incoming bits
					result |= currentByte & ((1u << bitsAvailable) - 1);    //OR in the bits
					neededBits -= bitsAvailable;
					currentByte = stream.CheckedReadByte();
					bitsAvailable = 8;
				}

				if (neededBits > 0)     //Left over bits
				{
					result <<= neededBits;
					bitsAvailable -= neededBits;
					result |= ((ulong)currentByte >> bitsAvailable) & ((1ul << neededBits) - 1);
				}

				yield return (long)result;
			}
		}

		public static long ReadVarIntUnsigned(this Stream stream)
		{
			long result = 0;
			long currentByte;
			int bitCount = 0;
			do
			{
				currentByte = stream.CheckedReadByte();
				result |= (currentByte & 0x7f) << bitCount;
				bitCount += 7;
			}
			while (currentByte >= 0x80);        //Done when the high bit is not set

			return result;
		}

		public static long ReadVarIntSigned(this Stream stream)
		{
			var unsigned = ReadVarIntUnsigned(stream);
			return unsigned.ZigzagDecode();
		}

		public static BigInteger? ReadBigVarInt(this Stream stream)
		{
			BigInteger result=BigInteger.Zero;
			long currentLong = 0;
			long currentByte;
			int bitCount = 0;
			do
			{
				currentByte = stream.ReadByte();
				if (currentByte < 1)
					return null;		//Reached the end of the stream

				currentLong |= (currentByte & 0x7f) << (bitCount % 63);
				bitCount += 7;

				if (bitCount % 63 == 0)
				{
					if (bitCount == 63)
						result = new BigInteger(currentLong);
					else
						result |= new BigInteger(currentLong) << (bitCount - 63);

					currentLong = 0;
				}
			}
			while (currentByte >= 0x80);        //Done when the high bit is not set

			if (currentLong != 0)      //Some bits left to add to result
			{
				var shift = (bitCount / 63) * 63;
				result |= new BigInteger(currentLong) << shift;
			}

			//Un zig-zag
			if((result&0x1) == 1)       //Can this be optimized?
			{
				result++;
				result = -result;
			}
			result >>= 1;

			return result;
		}
	}
}
