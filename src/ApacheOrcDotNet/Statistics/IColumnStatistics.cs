using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Statistics
{
    public interface IColumnStatistics
    {
		/// <summary>
		/// Number of values in a column. Doesn't include null values and only includes distinct dictionary values.
		/// </summary>
		long NumberOfValues { get; }

		/// <summary>
		/// Do any rows contain null values for this column?
		/// </summary>
		bool HasNulls { get; }
    }
}
