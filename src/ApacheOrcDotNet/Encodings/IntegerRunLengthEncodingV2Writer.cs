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

		public void Write(ArraySegment<long> values, bool areSigned, bool aligned)
		{
			while (values.Count > 0)
			{
				var window = values.CreateWindow(512);      //Encode a maximum of 512 values
				var numValuesEncoded = EncodeValues(window, areSigned, aligned);
				values = values.TakeValues(numValuesEncoded);
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

		int EncodeValues(ArraySegment<long> values, bool areSigned, bool aligned)
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
				DirectEncode(directZigZaggedValues, aligned, fixedBitWidth);
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
				DirectEncode(directZigZaggedValues, aligned, fixedBitWidth);
				return values.Count;
			}

			//At this point we must zigzag
			var zigZaggedValues = areSigned ? values.ZigzagEncode() : values;

			if (TryPatchEncoding(zigZaggedValues, values, minValue, ref fixedBitWidth, out length))
			{
				return length;
			}

			//If all else fails, DIRECT encode
			DirectEncode(zigZaggedValues, aligned, fixedBitWidth);
			return values.Count;
		}

		void FindRepeatedValues(ArraySegment<long> values, out long repeatingValue, out int length)
		{
			length = 0;
			repeatingValue = values.Array[values.Offset];
			foreach (var value in values)
			{
				if (repeatingValue != value)
					break;
				length++;
			}
		}

		enum DeltaEncodingResult { Success, Overflow, NonMonotonic}
		DeltaEncodingResult TryDeltaEncoding(ArraySegment<long> values, bool areSigned, bool aligned, out int length, out long minValue)
		{
			var array = values.Array;
			var offset = values.Offset;

			var deltas = new long[values.Count - 1];
			long initialValue = array[offset];
			minValue = initialValue;						//This gets saved for the patch base if things don't work out here
			long maxValue = initialValue;
			long initialDelta = array[offset + 1] - initialValue;
			long curDelta = initialDelta;
			long deltaMax = 0;      //This is different from the java implementation.  I believe their implementation may be a bug.
									//The first delta value is not considered for the delta bit width, so don't include it in the max value calculation
			bool isIncreasing = initialDelta > 0;
			bool isDecreasing = initialDelta < 0;
			bool isConstantDelta = true;
			deltas[0] = initialDelta;

			long previousValue = initialValue;
			int i = 1;
			foreach (var value in values.Skip(1))
			{
				curDelta = value - previousValue;
				if (value < minValue)
					minValue = value;
				if (value > maxValue)
					maxValue = value;

				if (value < previousValue)
					isIncreasing = false;
				if (previousValue > value)
					isDecreasing = false;

				if (curDelta != initialDelta)
					isConstantDelta = false;

				if (i > 1)     //Don't rewrite the first value because it holds the sign of the remaining values
				{
					var absCurrDelta = Math.Abs(curDelta);
					deltas[i - 1] = absCurrDelta;
					if (absCurrDelta > deltaMax)
						deltaMax = absCurrDelta;
				}

				i++;
				previousValue = value;
			}

			if (BitManipulation.SubtractionWouldOverflow(maxValue, minValue))
			{
				length = 0;
				return DeltaEncodingResult.Overflow;
			}

			if (maxValue == minValue)   //All values were identical
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

		bool TryPatchEncoding(ArraySegment<long> zigZagValues, ArraySegment<long> values, long minValue, ref int? fixedBitWidth, out int length)
		{
			var zigZagValuesHistogram = zigZagValues.GenerateHistogramOfBitWidths();
			var zigZagHundredthBits = BitManipulation.GetBitsRequiredForPercentile(zigZagValuesHistogram, zigZagValues.Count, 1.0);
			fixedBitWidth = zigZagHundredthBits;			//We'll use this later if if end up DIRECT encoding
			var zigZagNinetiethBits = BitManipulation.GetBitsRequiredForPercentile(zigZagValuesHistogram, zigZagValues.Count, 0.9);
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
			var baseReducedHundredthBits = BitManipulation.GetBitsRequiredForPercentile(baseReducedValuesHistogram, values.Count, 1.0);
			var baseReducedNinetyfifthBits = BitManipulation.GetBitsRequiredForPercentile(baseReducedValuesHistogram, values.Count, 0.95);
			if(baseReducedHundredthBits-baseReducedNinetyfifthBits==0)
			{
				//In the end, no benefit could be realized from patching
				length = 0;
				return false;
			}

			PatchEncode(minValue, baseReducedValues, baseReducedHundredthBits, baseReducedNinetyfifthBits);
			length = values.Count;
			return true;
		}

		bool SequenceIsTooShort(ArraySegment<long> values)
		{
			if (values.Count <= 3)
				return true;
			else
				return false;
		}

		void DirectEncode(ArraySegment<long> values, bool aligned, int? precalculatedFixedBitWidth)
		{
			int fixedBitWidth;
			if (precalculatedFixedBitWidth.HasValue)
				fixedBitWidth = precalculatedFixedBitWidth.Value;
			else
			{
				var histogram = values.GenerateHistogramOfBitWidths();
				fixedBitWidth = BitManipulation.GetBitsRequiredForPercentile(histogram, values.Count, 1.0);
			}

			if (aligned)
				fixedBitWidth = BitManipulation.FindNearestAlignedDirectWidth(fixedBitWidth);
			else
				fixedBitWidth = BitManipulation.FindNearestDirectWidth(fixedBitWidth);
			var encodedFixedBitWidth = fixedBitWidth.EncodeDirectWidth();

			var numValues = values.Count - 1;

			int byte1 = 0;
			byte1 |= 0x1 << 6;								//7..6 Encoding Type
			byte1 |= (encodedFixedBitWidth & 0x1f) << 1;	//5..1 Fixed Width
			byte1 |= numValues >> 8;						//0    MSB of length
			int byte2 = numValues&0xff;						//7..0 LSBs of length

			_outputStream.WriteByte((byte)byte1);
			_outputStream.WriteByte((byte)byte2);
			_outputStream.WriteBitpackedIntegers(values, fixedBitWidth);
		}

		void ShortRepeatEncode(long value, int repeatCount)
		{
			var bits = BitManipulation.NumBits((ulong)value);
			var width = bits / 8;
			if (width * 8 != bits)
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
			int encodedBitWidth = 0;
			if (deltaBitWidth > 0)
				encodedBitWidth = deltaBitWidth.EncodeDirectWidth();

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

		private void PatchEncode(long baseValue, long[] baseReducedValues, int baseReducedHundredthBits, int baseReducedNinetyfifthBits)
		{
		}
	}
}
