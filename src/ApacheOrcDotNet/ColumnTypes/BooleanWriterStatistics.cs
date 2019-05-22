using ApacheOrcDotNet.Statistics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
	public class BooleanWriterStatistics : ColumnWriterStatistics, IStatistics
	{
		public ulong? FalseCount { get; set; } = 0;
		public ulong? TrueCount { get; set; } = 0;
		public ulong NumValues { get; set; } = 0;
		public bool HasNull { get; set; } = false;

        public void AddValue(bool? value)
		{
			if (!value.HasValue)
				HasNull = true;
			else
			{
				if (value.Value)
					TrueCount = (TrueCount ?? 0) + 1;
				else
					FalseCount = (FalseCount ?? 0) + 1;
				NumValues++;
			}
		}

		public void FillColumnStatistics(ColumnStatistics columnStatistics)
		{
			//TODO BucketStatistics are stored in a List, so how can FalseCount be optional?  Just use zeros for now
			if (columnStatistics.BucketStatistics == null)
			{
				columnStatistics.BucketStatistics = new BucketStatistics
				{
					Count = new List<ulong>
					{
						0,
						0
					}
				};
			}

			columnStatistics.BucketStatistics.Count[0] += FalseCount ?? 0;
			columnStatistics.BucketStatistics.Count[1] += TrueCount ?? 0;

			columnStatistics.NumberOfValues += NumValues;
			if (HasNull)
				columnStatistics.HasNull = true;
		}
	}
}
