using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Statistics
{
    public interface IBooleanStatistics
    {
		ulong FalseCount { get; }
		ulong TrueCount { get; }
    }
}
