using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Encodings
{
    public class VarIntWriter
    {
		readonly Stream _outputStream;

		public VarIntWriter(Stream outputStream)
		{
			_outputStream = outputStream;
		}

		public void Write(IList<Tuple<uint,uint,uint,bool>> values)
		{
			foreach(var tuple in values)
			{
				_outputStream.WriteVarIntSigned(tuple.Item1, tuple.Item2, tuple.Item3, tuple.Item4);
			}
		}

		public void Write(IList<long> values)
		{
			foreach(var value in values)
			{
				_outputStream.WriteVarIntSigned(value);
			}
		}
    }
}
