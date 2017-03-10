using ApacheOrcDotNet.Statistics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
	public class BooleanWriterStatistics : ColumnWriterStatistics, IStatistics
	{
		public ulong FalseCount { get; set; } = 0;
		public ulong TrueCount { get; set; } = 0;
		public ulong NumValues { get; set; } = 0;
		public bool HasNull { get; set; } = false;

		public void AddValue(bool? value)
		{
			if (!value.HasValue)
				HasNull = true;
			else
			{
				if (value.Value)
					TrueCount++;
				else
					FalseCount++;
			}
			NumValues++;
		}

		public void FillColumnStatistics(ColumnStatistics columnStatistics)
		{
			if (columnStatistics.BucketStatistics == null)
			{
				columnStatistics.BucketStatistics = new BucketStatistics
				{
					Count = new List<ulong>
					{
						FalseCount,
						TrueCount
					}
				};
			}
			else
			{
				columnStatistics.BucketStatistics.Count[0] += FalseCount;
				columnStatistics.BucketStatistics.Count[1] += TrueCount;
			}

			columnStatistics.NumberOfValues += NumValues;
			if (HasNull)
				columnStatistics.HasNull = true;
		}
	}
}
