using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Encodings
{
    public class ByteRunLengthEncodingReader
    {
		readonly Stream _inputStream;

		public ByteRunLengthEncodingReader(Stream inputStream)
		{
			_inputStream = inputStream;
		}

		public IEnumerable<byte> Read()
		{
			while(true)
			{
				int firstByte = _inputStream.ReadByte();
				if (firstByte < 0)  //No more data available
					yield break;

				if(firstByte < 0x80)    //A run
				{
					var numBytes = firstByte + 3;
					var repeatedByte = _inputStream.CheckedReadByte();
					for (int i = 0; i < numBytes; i++)
						yield return repeatedByte;
				}
				else  //Literals
				{
					var numBytes = 0x100 - firstByte;
					for (int i = 0; i < numBytes; i++)
						yield return _inputStream.CheckedReadByte();
				}
			}
		}
    }
}
