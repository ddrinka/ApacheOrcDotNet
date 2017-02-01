using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Encodings
{
	public class IntegerRunLengthEncodingV2Writer
	{
		enum EncodingType { ShortRepeat, Direct, PatchedBase, Delta }

		public IntegerRunLengthEncodingV2Writer(Stream outputStream)
		{

		}


		public void Write(ArraySegment<long> values, bool areSigned)
		{
			ArraySegment<long> encodedValues;
			if (areSigned)
				encodedValues = ZigZagEncodeValues(values);
			else
				encodedValues = values;

			while (encodedValues.Count > 0)
			{
				EncodeValues(ref encodedValues);
			}
		}

		ArraySegment<long> ZigZagEncodeValues(ArraySegment<long> values)
		{
			var result = new long[values.Count];
			for (int i = 0; i < values.Count; i++)
			{
				var signedValue = values.Array[values.Offset + i];
				result[i] = signedValue.ZigzagEncode();
			}
			return new ArraySegment<long>(result);
		}

		void EncodeValues(ref ArraySegment<long> values)
		{
			//Find the longest monotonically increasing or decreasing (or constant) segment of data in the next 1024 samples
			//If the length is less than 10 and is constant, use SHORT_REPEAT
			//For data before and after segment, consider using PATCHED_BASE.  Otherwise fall back on DIRECT.
		}
		EncodingType IdentifyOptimalEncodingType(long[] values, out int fixedBitsRequired)
		{
			//Based on algorithm used in Orc's RunLengthIntegerWriterV2.java
			fixedBitsRequired = CalculateFixedBitsRequiredForDirectEncoding(values);

			if (SequenceIsTooShort(values))
				return EncodingType.Direct;

			throw new NotImplementedException();
		}

		int CalculateFixedBitsRequiredForDirectEncoding(long[] values)
		{
			int maxBits = 0;
			for (int i = 0; i < values.Length; i++)
			{
				var numBits = BitManipulation.FindNearestDirectWidth(BitManipulation.NumBits((ulong)values[i]));
				if (numBits > maxBits)
					maxBits = numBits;
			}
			return maxBits;
		}

		bool SequenceIsTooShort(long[] values)
		{
			if (values.Length <= 3)
				return true;
			else
				return false;
		}
    }
}
