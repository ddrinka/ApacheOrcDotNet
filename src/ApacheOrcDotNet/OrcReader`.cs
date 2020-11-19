﻿using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace ApacheOrcDotNet {
    public class OrcReader<T> where T : new()
    {
        readonly OrcReader _underlyingOrcReader;

        public OrcReader(Stream inputStream)
        {
            _underlyingOrcReader = new OrcReader(typeof(T), inputStream);
        }

        public IEnumerable<T> Read() => _underlyingOrcReader.Read().Cast<T>();
    }
}
