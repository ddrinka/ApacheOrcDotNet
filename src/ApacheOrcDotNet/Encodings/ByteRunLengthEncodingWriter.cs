using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Encodings
{
	public class ByteRunLengthEncodingWriter
	{
		readonly Stream _outputStream;

		public ByteRunLengthEncodingWriter(Stream outputStream)
		{
			_outputStream = outputStream;
		}

		public void Write(ArraySegment<byte> values)
		{
			while (values.Count > 0)
			{
				//Check for repeats
				byte repeatingValue;
				var repeatingValueCount = FindRepeatedValues(values, out repeatingValue);
				if (repeatingValueCount >= 3)
				{
					EncodeRepeat(repeatingValueCount, repeatingValue);
					values = values.TakeValues(repeatingValueCount);
					continue;   //Search again for new repeating values
				}

				//Check for future repeats
				var repeatLocation = FindNonRepeatingValues(values);
				var literalWindow = values.CreateWindow(repeatLocation);
				EncodeLiterals(literalWindow);
				values = values.TakeValues(repeatLocation);
			}
		}

		void EncodeRepeat(int repeatingValueCount, byte repeatingValue)
		{
			var byte1 = (byte)(repeatingValueCount - 3);

			_outputStream.WriteByte(byte1);
			_outputStream.WriteByte(repeatingValue);
		}

		void EncodeLiterals(ArraySegment<byte> values)
		{
			var byte1 = (byte)-values.Count;

			_outputStream.WriteByte(byte1);
			_outputStream.Write(values.Array, values.Offset, values.Count);
		}

		int FindNonRepeatingValues(ArraySegment<byte> values)
		{
			if (values.Count < 3)
				return values.Count;

			int result = 0;
			while (result < values.Count - 2 && result < 128 - 2)
			{
				var val0 = values.Array[values.Offset + result + 0];
				var val1 = values.Array[values.Offset + result + 1];
				var val2 = values.Array[values.Offset + result + 2];
				if (val0 == val1 && val0 == val2)
					return result;       //End of the non-repeating section
				result++;
			}

			return result + 2;			//No repeats found including the last two values
		}

		int FindRepeatedValues(ArraySegment<byte> values, out byte repeatingValue)
		{
			int result = 0;
			repeatingValue = values.Array[values.Offset];
			while (result < values.Count && result < 127 + 3)
			{
				if (values.Array[values.Offset + result] != repeatingValue)
					break;
				result++;
			}

			return result;
		}
	}
}
