using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet
{
    public interface IValueEncoder<T>
    {
		void EncodeValues(IEnumerable<T> values, Stream outputStream);
    }
}
