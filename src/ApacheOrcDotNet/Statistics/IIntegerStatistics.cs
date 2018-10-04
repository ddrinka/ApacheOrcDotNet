using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Statistics
{
    public interface IIntegerStatistics
    {
		long? Minimum { get; }
		long? Maximum { get; }

		/// <summary>
		/// Sum of all values in column or null if the value overflowed
		/// </summary>
		long? Sum { get; }
    }
}
