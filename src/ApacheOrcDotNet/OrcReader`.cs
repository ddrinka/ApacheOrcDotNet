using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ApacheOrcDotNet
{
    public class OrcReader<T> where T : new()
    {
        readonly OrcReader _underlyingOrcReader;

        public OrcReader(Stream inputStream, bool ignoreMissingColumns = false)
        {
            _underlyingOrcReader = new OrcReader(typeof(T), inputStream, ignoreMissingColumns);
        }

        public IEnumerable<T> Read() => _underlyingOrcReader.Read().Cast<T>();
    }
}
