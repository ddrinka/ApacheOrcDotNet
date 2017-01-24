using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Encodings
{
	public class IntegerRunLengthEncodingV2Reader
	{
		enum EncodingType { ShortRepeat, Direct, PatchedBase, Delta }

		readonly Stream _inputStream;

		public IntegerRunLengthEncodingV2Reader(Stream inputStream)
		{
			_inputStream = inputStream;
		}

		public IEnumerable<int> ReadAsInt()
		{
			foreach (var value in ReadValues(true))
			{
				if (value > Int32.MaxValue || value < Int32.MinValue)
					throw new OverflowException($"Encoded number ({value}) too big for an Int");

				yield return (int)value;
			}
		}

		public IEnumerable<uint> ReadAsUInt()
		{
			foreach (var value in ReadValues(false))
			{
				if (value > UInt32.MaxValue)
					throw new OverflowException($"Encoded number ({value}) too big for a UInt");

				yield return (uint)value;
			}
		}

		public IEnumerable<long> ReadAsLong()
		{
			return ReadValues(true);
		}

		public IEnumerable<ulong> ReadAsULong()
		{
			foreach(var value in ReadValues(false))
			{
				yield return (ulong)value;
			}
		}

		IEnumerable<long> ReadValues(bool isSigned)
		{
			while (true)
			{
				int firstByte = _inputStream.ReadByte();
				if (firstByte < 0)  //No more data available
					yield break;

				var encodingType = (EncodingType)((firstByte >> 6) & 0x3);
				switch (encodingType)
				{
					case EncodingType.ShortRepeat:
						foreach (var value in ReadShortRepeatValues(firstByte, isSigned))
							yield return value;
						break;
					case EncodingType.Direct:
						foreach (var value in ReadDirectValues(firstByte, isSigned))
							yield return value;
						break;
					case EncodingType.PatchedBase:
						foreach (var value in ReadPatchedBaseValues(firstByte, isSigned))
							yield return value;
						break;
					case EncodingType.Delta:
						foreach (var value in ReadDeltaValues(firstByte, isSigned))
							yield return value;
						break;
				}
			}
		}

		IEnumerable<long> ReadShortRepeatValues(int firstByte, bool isSigned)
		{
			var width = ((firstByte >> 3) & 0x7) + 1;
			var repeatCount = (firstByte & 0x7) + 3;
			var value = _inputStream.ReadLongBE(width);
			if (isSigned)
				value = value.ZigzagDecode();
			for (int i = 0; i < repeatCount; i++)
				yield return value;
		}

		IEnumerable<long> ReadDirectValues(int firstByte, bool isSigned)
		{
			var encodedWidth = (firstByte >> 1) & 0x1f;
			var width = encodedWidth.DecodeDirectWidth();
			int length = (firstByte & 0x1) << 8;
			length |= _inputStream.CheckedReadByte();
			length += 1;
			foreach(var value in _inputStream.ReadBitpackedIntegers(width, length))
			{
				if (isSigned)
					yield return value.ZigzagDecode();
				else
					yield return value;
			}
		}
	}
}
