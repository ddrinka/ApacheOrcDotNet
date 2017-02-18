using ApacheOrcDotNet.Infrastructure;
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

		public void Write(IList<byte> values)
		{
			var position = 0;
			while (position < values.Count)
			{
				var window = new ListSegment<byte>(values, position);
				//Check for repeats
				byte repeatingValue;
				var repeatingValueCount = FindRepeatedValues(window, out repeatingValue);
				if (repeatingValueCount >= 3)
				{
					EncodeRepeat(repeatingValueCount, repeatingValue);
					position += repeatingValueCount;
					continue;   //Search again for new repeating values
				}

				//Check for future repeats
				var repeatLocation = FindNonRepeatingValues(window);
				var literalWindow = new ListSegment<byte>(window, 0, repeatLocation);
				EncodeLiterals(literalWindow);
				position += repeatLocation;
			}
		}

		void EncodeRepeat(int repeatingValueCount, byte repeatingValue)
		{
			var byte1 = (byte)(repeatingValueCount - 3);

			_outputStream.WriteByte(byte1);
			_outputStream.WriteByte(repeatingValue);
		}

		void EncodeLiterals(IList<byte> values)
		{
			var byte1 = (byte)-values.Count;

			_outputStream.WriteByte(byte1);
			foreach (var curByte in values)
				_outputStream.WriteByte(curByte);
		}

		int FindNonRepeatingValues(IList<byte> values)
		{
			if (values.Count < 3)
				return values.Count;

			int result = 0;
			while (result < values.Count - 2 && result < 128 - 2)
			{
				var val0 = values[result + 0];
				var val1 = values[result + 1];
				var val2 = values[result + 2];
				if (val0 == val1 && val0 == val2)
					return result;       //End of the non-repeating section
				result++;
			}

			return result + 2;			//No repeats found including the last two values
		}

		int FindRepeatedValues(IList<byte> values, out byte repeatingValue)
		{
			int result = 0;
			repeatingValue = values[0];
			while (result < values.Count && result < 127 + 3)
			{
				if (values[result] != repeatingValue)
					break;
				result++;
			}

			return result;
		}
	}
}
