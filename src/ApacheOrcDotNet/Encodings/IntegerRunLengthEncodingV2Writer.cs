using ApacheOrcDotNet.Infrastructure;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Encodings
{
	public class IntegerRunLengthEncodingV2Writer
	{
		readonly Stream _outputStream;

		public IntegerRunLengthEncodingV2Writer(Stream outputStream)
		{
			_outputStream = outputStream;
		}

		public void Write(IList<long> values, bool areSigned, bool aligned)
		{
			var position = 0;
			while (position < values.Count)
			{
				var window = new ListSegment<long>(values, position, 512);      //Encode a maximum of 512 values
				var numValuesEncoded = EncodeValues(window, areSigned, aligned);
				position += numValuesEncoded;
			}
		}

		int EncodeValues(IList<long> values, bool areSigned, bool aligned)
		{
			//Eventually:
			//Find the longest monotonically increasing or decreasing (or constant) segment of data in the next 1024 samples
			//If the length is less than 10 and is constant, use SHORT_REPEAT
			//For data before and after segment, consider using PATCHED_BASE.  Otherwise fall back on DIRECT.

			//For now, match the Java implementation
			//Count how many values repeat in the next 512 samples
			//If it's less than 10 and more than 3, use SHORT_REPEAT
			//Otherwise, try to use DELTA
			//If values aren't monotonically increasing or decreasing, check if PATCHED_BASE makes sense (90% of the values are one bit-width less than the 100% number)
			//If all else fails, use DIRECT
			int? fixedBitWidth = null;

			if (SequenceIsTooShort(values))
			{
				var directZigZaggedValues = areSigned ? values.ZigzagEncode() : values;
				DirectEncode(directZigZaggedValues, values.Count, aligned, fixedBitWidth);
				return values.Count;
			}

			int length;
			long repeatingValue;
			FindRepeatedValues(values, out repeatingValue, out length);
			if (length >= 3 && length <= 10)
			{
				ShortRepeatEncode(areSigned ? repeatingValue.ZigzagEncode() : repeatingValue, length);
				return length;
			}

			long minValue;
			var result = TryDeltaEncoding(values, areSigned, aligned, out length, out minValue);
			if (result == DeltaEncodingResult.Success)
			{
				return length;
			}
			else if (result == DeltaEncodingResult.Overflow)
			{
				var directZigZaggedValues = areSigned ? values.ZigzagEncode() : values;
				DirectEncode(directZigZaggedValues, values.Count, aligned, fixedBitWidth);
				return values.Count;
			}

			//At this point we must zigzag
			var zigZaggedValues = areSigned ? values.ZigzagEncode() : values;

			if (TryPatchEncoding(zigZaggedValues, values, minValue, ref fixedBitWidth, out length))
			{
				return length;
			}

			//If all else fails, DIRECT encode
			DirectEncode(zigZaggedValues, values.Count, aligned, fixedBitWidth);
			return values.Count;
		}

		void FindRepeatedValues(IEnumerable<long> values, out long repeatingValue, out int length)
		{
			length = 0;
			repeatingValue = 0;

			bool isFirst = true;
			foreach (var value in values)
			{
				if (isFirst)
				{
					repeatingValue = value;
					isFirst = false;
				}
				else if (repeatingValue != value)
					break;

				length++;
			}
		}

		enum DeltaEncodingResult { Success, Overflow, NonMonotonic}
		DeltaEncodingResult TryDeltaEncoding(IList<long> values, bool areSigned, bool aligned, out int length, out long minValue)
		{
			var deltas = new long[values.Count - 1];
			long initialValue = values[0];
			minValue = initialValue;						//This gets saved for the patch base if things don't work out here
			long maxValue = initialValue;
			long initialDelta = values[1] - initialValue;
			long curDelta = initialDelta;
			long deltaMax = 0;      //This is different from the java implementation.  I believe their implementation may be a bug.
									//The first delta value is not considered for the delta bit width, so don't include it in the max value calculation
			bool isIncreasing = initialDelta > 0;
			bool isDecreasing = initialDelta < 0;
			bool isConstantDelta = true;

			long previousValue = values[1];
			if (values[1] < minValue)
				minValue = values[1];
			if (values[1] > maxValue)
				maxValue = values[1];

			deltas[0] = initialDelta;

			int i = 2;
			foreach (var value in values.Skip(2))	//The first value is initialValue. The second value is initialDelta, already loaded. Start with the third value
			{
				curDelta = value - previousValue;
				if (value < minValue)
					minValue = value;
				if (value > maxValue)
					maxValue = value;

				if (value < previousValue)
					isIncreasing = false;
				if (value > previousValue)
					isDecreasing = false;

				if (curDelta != initialDelta)
					isConstantDelta = false;

				var absCurrDelta = Math.Abs(curDelta);
				deltas[i - 1] = absCurrDelta;
				if (absCurrDelta > deltaMax)
					deltaMax = absCurrDelta;

				i++;
				previousValue = value;
			}

			if (BitManipulation.SubtractionWouldOverflow(maxValue, minValue))
			{
				length = 0;
				return DeltaEncodingResult.Overflow;
			}

			if (maxValue == minValue)   //All values after the first were identical
			{
				DeltaEncode(minValue, areSigned, values.Count);
				length = values.Count;
				return DeltaEncodingResult.Success;
			}

			if(isConstantDelta) //All values changed by set amount
			{
				DeltaEncode(initialValue, areSigned, curDelta, values.Count);
				length = values.Count;
				return DeltaEncodingResult.Success;
			}

			if(isIncreasing || isDecreasing)
			{
				var deltaBits = BitManipulation.NumBits((ulong)deltaMax);
				if (aligned)
					deltaBits = BitManipulation.FindNearestAlignedDirectWidth(deltaBits);
				else
					deltaBits = BitManipulation.FindNearestDirectWidth(deltaBits);

				DeltaEncode(initialValue, areSigned, values.Count, deltas, deltaBits);
				length = values.Count;
				return DeltaEncodingResult.Success;
			}

			length = 0;
			return DeltaEncodingResult.NonMonotonic;
		}

		bool TryPatchEncoding(IEnumerable<long> zigZagValues, IList<long> values, long minValue, ref int? fixedBitWidth, out int length)
		{
			var zigZagValuesHistogram = zigZagValues.GenerateHistogramOfBitWidths();
			var zigZagHundredthBits = BitManipulation.GetBitsRequiredForPercentile(zigZagValuesHistogram, 1.0);
			fixedBitWidth = zigZagHundredthBits;            //We'll use this later if if end up DIRECT encoding
			var zigZagNinetiethBits = BitManipulation.GetBitsRequiredForPercentile(zigZagValuesHistogram, 0.9);
			if (zigZagHundredthBits - zigZagNinetiethBits == 0)
			{
				//Requires as many bits even if we eliminate 10% of the most difficult values
				length = 0;
				return false;
			}

			var baseReducedValues = new long[values.Count];
			int i = 0;
			foreach (var value in values)
				baseReducedValues[i++] = value - minValue;

			var baseReducedValuesHistogram = baseReducedValues.GenerateHistogramOfBitWidths();
			var baseReducedHundredthBits = BitManipulation.GetBitsRequiredForPercentile(baseReducedValuesHistogram, 1.0);
			var baseReducedNinetyfifthBits = BitManipulation.GetBitsRequiredForPercentile(baseReducedValuesHistogram, 0.95);
			if (baseReducedHundredthBits - baseReducedNinetyfifthBits == 0)
			{
				//In the end, no benefit could be realized from patching
				length = 0;
				return false;
			}

			PatchEncode(minValue, baseReducedValues, baseReducedHundredthBits, baseReducedNinetyfifthBits);
			length = values.Count;
			return true;
		}

		bool SequenceIsTooShort(IList<long> values)
		{
			if (values.Count <= 3)
				return true;
			else
				return false;
		}

		void DirectEncode(IEnumerable<long> values, int numValues, bool aligned, int? precalculatedFixedBitWidth)
		{
			int fixedBitWidth;
			if (precalculatedFixedBitWidth.HasValue)
				fixedBitWidth = precalculatedFixedBitWidth.Value;
			else
			{
				var histogram = values.GenerateHistogramOfBitWidths();
				fixedBitWidth = BitManipulation.GetBitsRequiredForPercentile(histogram, 1.0);
			}

			if (aligned)
				fixedBitWidth = BitManipulation.FindNearestAlignedDirectWidth(fixedBitWidth);
			else
				fixedBitWidth = BitManipulation.FindNearestDirectWidth(fixedBitWidth);
			var encodedFixedBitWidth = fixedBitWidth.EncodeDirectWidth();

			int byte1 = 0;
			byte1 |= 0x1 << 6;								//7..6 Encoding Type
			byte1 |= (encodedFixedBitWidth & 0x1f) << 1;	//5..1 Fixed Width
			byte1 |= (numValues - 1) >> 8;					//0    MSB of length
			int byte2 = (numValues - 1) & 0xff;				//7..0 LSBs of length

			_outputStream.WriteByte((byte)byte1);
			_outputStream.WriteByte((byte)byte2);
			_outputStream.WriteBitpackedIntegers(values, fixedBitWidth);
		}

		void ShortRepeatEncode(long value, int repeatCount)
		{
			var bits = BitManipulation.FindNearestDirectWidth(BitManipulation.NumBits((ulong)value));
			var width = bits / 8;
			if (bits % 8 != 0)
				width++;      //Some remainder

			int byte1 = 0;
			byte1 |= 0x0 << 6;
			byte1 |= (width - 1) << 3;
			byte1 |= repeatCount - 3;

			_outputStream.WriteByte((byte)byte1);
			_outputStream.WriteLongBE(width, value);
		}

		void DeltaEncode(long initialValue, bool areSigned, int repeatCount)
		{
			DeltaEncode(initialValue, areSigned, 0, repeatCount);
		}

		void DeltaEncode(long initialValue, bool areSigned, long constantOffset, int repeatCount)
		{
			DeltaEncode(initialValue, areSigned, repeatCount, new[] { constantOffset }, 0);
		}

		void DeltaEncode(long initialValue, bool areSigned, int numValues, long[] deltas, int deltaBitWidth)
		{
			if (deltaBitWidth == 1)
				deltaBitWidth = 2;      //encodedBitWidth of zero is reserved for constant runlengths. Allocate an extra bit to avoid triggering that logic.

			int encodedBitWidth = deltaBitWidth > 1 ? deltaBitWidth.EncodeDirectWidth() : 0;

			int byte1 = 0;
			byte1 |= 0x3 << 6;                              //7..6 Encoding Type
			byte1 |= (encodedBitWidth & 0x1f) << 1;         //5..1 Delta Bit Width
			byte1 |= (numValues - 1) >> 8;                  //0    MSB of length
			int byte2 = (numValues - 1) & 0xff;             //7..0 LSBs of length

			_outputStream.WriteByte((byte)byte1);
			_outputStream.WriteByte((byte)byte2);
			if (areSigned)
				_outputStream.WriteVarIntSigned(initialValue);                          //Base Value
			else
				_outputStream.WriteVarIntUnsigned(initialValue);
			_outputStream.WriteVarIntSigned(deltas[0]);                                 //Delta Base
			if (deltas.Length > 1)
				_outputStream.WriteBitpackedIntegers(deltas.Skip(1), deltaBitWidth);    //Delta Values
		}

		void PatchEncode(long baseValue, long[] baseReducedValues, int originalBitWidth, int reducedBitWidth)
		{
			var baseIsNegative = baseValue < 0;
			if (baseIsNegative)
				baseValue = -baseValue;
			var numBitsBaseValue = BitManipulation.NumBits((ulong)baseValue) + 1;   //Need one additional bit for the sign
			var numBytesBaseValue = numBitsBaseValue / 8;
			if (numBitsBaseValue % 8 != 0)
				numBytesBaseValue++;      //Some remainder
			if (baseIsNegative)
				baseValue |= 1L << ((numBytesBaseValue * 8) - 1);   //Set the MSB to 1 to mark the sign

			var patchBitWidth = BitManipulation.FindNearestDirectWidth(originalBitWidth - reducedBitWidth);
			if(patchBitWidth==64)
			{
				patchBitWidth = 56;
				reducedBitWidth = 8;
			}
			var encodedPatchBitWidth = patchBitWidth.EncodeDirectWidth();
			var valueBitWidth = BitManipulation.FindNearestDirectWidth(reducedBitWidth);
			var encodedValueBitWidth = valueBitWidth.EncodeDirectWidth();

			int gapBitWidth;
			var patchGapList = GeneratePatchList(baseReducedValues, patchBitWidth, reducedBitWidth, out gapBitWidth);
			var patchListBitWidth = BitManipulation.FindNearestDirectWidth(gapBitWidth + patchBitWidth);


			int byte1 = 0, byte2 = 0, byte3 = 0, byte4 = 0;
			byte1 |= 0x2 << 6;                                  //7..6 Encoding Type
			byte1 |= (encodedValueBitWidth & 0x1f) << 1;        //5..1 Value Bit Width
			byte1 |= (baseReducedValues.Length - 1) >> 8;       //0    MSB of length
			byte2 |= (baseReducedValues.Length - 1) & 0xff;		//7..0 LSBs of length
			byte3 |= (numBytesBaseValue - 1) << 5;              //7..5 Base Value Byte Width
			byte3 |= encodedPatchBitWidth & 0x1f;               //4..0 Encoded Patch Bit Width
			byte4 |= (gapBitWidth - 1) << 5;                    //7..5 Gap Bit Width
			byte4 |= patchGapList.Length & 0x1f;                //4..0 Patch/Gap List Length

			_outputStream.WriteByte((byte)byte1);
			_outputStream.WriteByte((byte)byte2);
			_outputStream.WriteByte((byte)byte3);
			_outputStream.WriteByte((byte)byte4);
			_outputStream.WriteLongBE(numBytesBaseValue, baseValue);
			_outputStream.WriteBitpackedIntegers(baseReducedValues, valueBitWidth);
			_outputStream.WriteBitpackedIntegers(patchGapList, patchListBitWidth);
		}

		long[] GeneratePatchList(long[] baseReducedValues, int patchBitWidth, int reducedBitWidth, out int gapBitWidth)
		{
			int prevIndex = 0;
			int maxGap = 0;

			long mask = (1L << reducedBitWidth) - 1;

			var estimatedPatchCount = (int)(baseReducedValues.Length * 0.05 + .5);      //We're patching 5% of the values (round up)
			var patchGapList = new List<Tuple<int, long>>(estimatedPatchCount);

			for(int i=0;i<baseReducedValues.Length;i++)
			{
				if(baseReducedValues[i]>mask)
				{
					var gap = i - prevIndex;
					if (gap > maxGap)
						maxGap = gap;

					var patch = (long)((ulong)baseReducedValues[i] >> reducedBitWidth);
					patchGapList.Add(Tuple.Create(gap, patch));

					baseReducedValues[i] &= mask;
					prevIndex = i;
				}
			}

			var actualLength = patchGapList.Count;

			if (maxGap == 0 && patchGapList.Count != 0)
				gapBitWidth = 1;
			else
				gapBitWidth = BitManipulation.FindNearestDirectWidth(BitManipulation.NumBits((ulong)maxGap));
			if (gapBitWidth > 8)
			{
				//Prepare for the special case of 511 and 256
				gapBitWidth = 8;
				if (maxGap == 511)
					actualLength += 2;
				else
					actualLength += 1;
			}

			int resultIndex = 0;
			var result = new long[actualLength];
			foreach(var patchGap in patchGapList)
			{
				long gap = patchGap.Item1;
				long patch = patchGap.Item2;
				while(gap>255)
				{
					result[resultIndex++] = 255L << patchBitWidth;
					gap -= 255;
				}
				result[resultIndex++] = gap << patchBitWidth | patch;
			}

			return result;
		}
	}
}
