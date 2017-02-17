using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Encodings
{
    public class BitWriter
    {
		readonly ByteRunLengthEncodingWriter _byteWriter;

		public BitWriter(Stream outputStream)
		{
			_byteWriter = new ByteRunLengthEncodingWriter(outputStream);
		}

		public void Write(ArraySegment<bool> values)
		{
			var numBytes = values.Count / 8;
			if (values.Count % 8 != 0)
				numBytes++;
			var bytes = new byte[numBytes];

			int byteIndex = 0;
			int bitIndex = 7;
			for (int i = 0; i < values.Count; i++)
			{
				if (values.Array[values.Offset + i])
					bytes[byteIndex] |= (byte)(1 << bitIndex);
				bitIndex--;

				if (bitIndex == -1)
				{
					bitIndex = 7;
					byteIndex++;
				}
			}

			_byteWriter.Write(new ArraySegment<byte>(bytes));
		}
	}
}
