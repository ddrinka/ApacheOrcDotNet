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
		readonly bool _isSigned;

		public IntegerRunLengthEncodingV2Reader(Stream inputStream, bool isSigned)
		{
			_inputStream = inputStream;
			_isSigned = isSigned;
		}

		public IEnumerable<long> Read()
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
						foreach (var value in ReadShortRepeatValues(firstByte))
							yield return value;
						break;
					case EncodingType.Direct:
						foreach (var value in ReadDirectValues(firstByte))
							yield return value;
						break;
					case EncodingType.PatchedBase:
						foreach (var value in ReadPatchedBaseValues(firstByte))
							yield return value;
						break;
					case EncodingType.Delta:
						foreach (var value in ReadDeltaValues(firstByte))
							yield return value;
						break;
				}
			}
		}

		IEnumerable<long> ReadShortRepeatValues(int firstByte)
		{
			var width = ((firstByte >> 3) & 0x7) + 1;
			var repeatCount = (firstByte & 0x7) + 3;
			var value = _inputStream.ReadLongBE(width);
			if (_isSigned)
				value = value.ZigzagDecode();
			for (int i = 0; i < repeatCount; i++)
				yield return value;
		}

		IEnumerable<long> ReadDirectValues(int firstByte)
		{
			var encodedWidth = (firstByte >> 1) & 0x1f;
			var width = encodedWidth.DecodeDirectWidth();
			int length = (firstByte & 0x1) << 8;
			length |= _inputStream.CheckedReadByte();
			length += 1;
			foreach (var value in _inputStream.ReadBitpackedIntegers(width, length))
			{
				if (_isSigned)
					yield return value.ZigzagDecode();
				else
					yield return value;
			}
		}

		IEnumerable<long> ReadPatchedBaseValues(int firstByte)
		{
			var encodedWidth = (firstByte >> 1) & 0x1f;
			var width = encodedWidth.DecodeDirectWidth();
			int length = (firstByte & 0x1) << 8;
			length |= _inputStream.CheckedReadByte();
			length += 1;

			var thirdByte = _inputStream.CheckedReadByte();
			var baseValueWidth = ((thirdByte >> 5) & 0x7) + 1;
			var encodedPatchWidth = thirdByte & 0x1f;
			var patchWidth = encodedPatchWidth.DecodeDirectWidth();

			var fourthByte = _inputStream.CheckedReadByte();
			var patchGapWidth = ((fourthByte >> 5) & 0x7) + 1;
			var patchListLength = fourthByte & 0x1f;

			long baseValue = _inputStream.ReadLongBE(baseValueWidth);
			long msbMask = (1L << ((baseValueWidth * 8) - 1));
			if ((baseValue & msbMask) != 0)
			{
				baseValue = baseValue & ~msbMask;
				baseValue = -baseValue;
			}

			//Buffer all the values so we can patch them
			var dataValues = _inputStream.ReadBitpackedIntegers(width, length).ToArray();

			if (patchGapWidth + patchWidth > 64)
				throw new InvalidDataException($"{nameof(patchGapWidth)} ({patchGapWidth}) + {nameof(patchWidth)} ({patchWidth}) > 64");

			var patchListWidth = BitManipulation.FindNearestDirectWidth(patchWidth + patchGapWidth);
			var patchListValues = _inputStream.ReadBitpackedIntegers(patchListWidth, patchListLength).ToArray();

			int patchIndex = 0;
			long gap = 0;
			long patch;
			GetNextPatch(patchListValues, ref patchIndex, ref gap, out patch, patchWidth, (1L << patchWidth) - 1);

			for (int i = 0; i < length; i++)
			{
				if (i == gap)
				{
					var patchedValue = dataValues[i] | (patch << width);
					yield return baseValue + patchedValue;

					if (patchIndex < patchListLength)
						GetNextPatch(patchListValues, ref patchIndex, ref gap, out patch, patchWidth, (1L << patchWidth) - 1);
				}
				else
					yield return baseValue + dataValues[i];
			}
		}

		void GetNextPatch(long[] patchListValues, ref int patchIndex, ref long gap, out long patch, int patchWidth, long patchMask)
		{
			var raw = patchListValues[patchIndex];
			patchIndex++;
			long curGap = (long)((ulong)raw >> patchWidth);
			patch = raw & patchMask;
			while (curGap == 255 && patch == 0)
			{
				gap += 255;
				raw = patchListValues[patchIndex];
				patchIndex++;
				curGap = (long)((ulong)raw >> patchWidth);
				patch = raw & patchMask;
			}
			gap += curGap;
		}

		IEnumerable<long> ReadDeltaValues(int firstByte)
		{
			var encodedWidth = (firstByte >> 1) & 0x1f;
			int width = 0;
			if (encodedWidth != 0)      //EncodedWidth 0 means Width 0 for Delta
				width = encodedWidth.DecodeDirectWidth();
			int length = (firstByte & 0x1) << 8;
			length |= _inputStream.CheckedReadByte();
			//Delta lengths start at 0

			long currentValue;
			if (_isSigned)
				currentValue = _inputStream.ReadVarIntSigned();
			else
				currentValue = _inputStream.ReadVarIntUnsigned();

			yield return currentValue;

			var deltaBase = _inputStream.ReadVarIntSigned();
			if (width == 0)
			{
				//Uses a fixed delta base for every value
				for (int i = 0; i < length; i++)
				{
					currentValue += deltaBase;
					yield return currentValue;
				}
			}
			else
			{
				currentValue += deltaBase;
				yield return currentValue;

				var deltaValues = _inputStream.ReadBitpackedIntegers(width, length - 1);
				foreach (var deltaValue in deltaValues)
				{
					if (deltaBase > 0)
					{
						currentValue += deltaValue;
						yield return currentValue;
					}
					else
					{
						currentValue -= deltaValue;
						yield return currentValue;
					}
				}
			}
		}
	}
}
