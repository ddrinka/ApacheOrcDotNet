using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Statistics
{
    public interface IStringStatistics
    {
		string Minimum { get; }
		string Maximum { get; }

		/// <summary>
		/// Total length of all strings
		/// </summary>
		long? Sum { get; }
    }
}
