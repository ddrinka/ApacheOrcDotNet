using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader
{
    public sealed class OrcReaderResultSet<T> where T : struct
    {
        public bool CopyTo(Span<T> destination)
        {
            throw new NotImplementedException();
        }
    }
}
