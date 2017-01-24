using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
			for(int i=numBytes-1;i>=0;i--)
			{
				long nextByte = stream.CheckedReadByte();
				result |= (nextByte << (i * 8));
			}
			return result;
		}

		public static long ZigzagDecode(this long value)
		{
			return (long)(((ulong)value) >> 1) ^ -(value & 1);
			//( value >> 1 ) ^ ( ~( value & 1 ) + 1 )
		}

		public static long ZigzagEncode(this long value)
		{
			return (value << 1) ^ (value >> 63);
		}

		public static int DecodeDirectWidth(this int encodedWidth)
		{
			if (encodedWidth >= 0 && encodedWidth <= 23)
				return encodedWidth + 1;
			switch(encodedWidth)
			{
				case 24:return 26;
				case 25:return 28;
				case 26:return 30;
				case 27:return 32;
				case 28:return 40;
				case 29:return 48;
				case 30:return 56;
				case 31:return 64;
			}
			throw new NotImplementedException($"Unimplemented {nameof(encodedWidth)} {encodedWidth}");
		}

		public static IEnumerable<long> ReadBitpackedIntegers(this Stream stream, int bitWidth, int count)
		{
			byte currentByte = 0;
			int bitsAvailable = 0;
			for(int i=0;i<count;i++)
			{
				ulong result = 0;
				int neededBits = bitWidth;
				while(neededBits > bitsAvailable)
				{
					result <<= bitsAvailable;	//Make space for incoming bits
					result |= currentByte & ((1u << bitsAvailable) - 1);    //
					neededBits -= bitsAvailable;
					currentByte = stream.CheckedReadByte();
					bitsAvailable = 8;
				}

				if (neededBits > 0)     //Left over bits
				{
					result <<= bitsAvailable;
					neededBits -= bitsAvailable;
					result |= ((ulong)currentByte >> bitsAvailable) & ((1ul << neededBits) - 1);
				}

				yield return (long)result;
			}
		}
    }
}
