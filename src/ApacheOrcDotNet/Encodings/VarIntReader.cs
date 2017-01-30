using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Encodings
{
    public class VarIntReader
    {
		readonly Stream _inputStream;

		public VarIntReader(Stream inputStream)
		{
			_inputStream = inputStream;
		}

		public IEnumerable<BigInteger> Read()
		{
			while(true)
			{
				var bigInt = BitManipulation.ReadBigVarInt(_inputStream);
				if (bigInt.HasValue)
					yield return bigInt.Value;
				else
					yield break;
			}
		}
    }
}
