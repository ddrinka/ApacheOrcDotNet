using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Encodings
{
    public class BitReader
    {
		readonly ByteRunLengthEncodingReader _byteReader;

		public BitReader(Stream inputStream)
		{
			_byteReader=new ByteRunLengthEncodingReader(inputStream);
		}

		public IEnumerable<bool> Read()
		{
			foreach(var b in _byteReader.Read())
			{
				if ((b & 0x80) != 0)
					yield return true;
				else
					yield return false;
				if ((b & 0x40) != 0)
					yield return true;
				else
					yield return false;
				if ((b & 0x20) != 0)
					yield return true;
				else
					yield return false;
				if ((b & 0x10) != 0)
					yield return true;
				else
					yield return false;
				if ((b & 0x08) != 0)
					yield return true;
				else
					yield return false;
				if ((b & 0x04) != 0)
					yield return true;
				else
					yield return false;
				if ((b & 0x02) != 0)
					yield return true;
				else
					yield return false;
				if ((b & 0x01) != 0)
					yield return true;
				else
					yield return false;
			}
		}
    }
}
