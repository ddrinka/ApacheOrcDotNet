using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Statistics
{
    public interface IDoubleStatistics
    {
		double? Minimum { get; }
		double? Maximum { get; }
		double? Sum { get; }
    }
}
