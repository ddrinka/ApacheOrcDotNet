using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ApacheOrcDotNet.Statistics;

namespace ApacheOrcDotNet.ColumnTypes
{
	public class StructWriterStatistics : ColumnWriterStatistics, IStatistics
	{
		public ulong NumValues { get; set; } = 0;

		public void FillColumnStatistics(ColumnStatistics columnStatistics)
		{
			columnStatistics.NumberOfValues += NumValues;
		}
	}
}
