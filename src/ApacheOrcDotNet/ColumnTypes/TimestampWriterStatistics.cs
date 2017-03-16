using ApacheOrcDotNet.Statistics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class TimestampWriterStatistics : ColumnWriterStatistics, IStatistics
    {
		public long Min { get; set; } = long.MaxValue;
		public long Max { get; set; } = long.MinValue;
		public ulong NumValues { get; set; } = 0;
		public bool HasNull { get; set; } = false;

		public void AddValue(long? millisecondsSinceEpoch)
		{
			if (!millisecondsSinceEpoch.HasValue)
				HasNull = true;
			else
			{
				if (millisecondsSinceEpoch > Max)
					Max = millisecondsSinceEpoch.Value;
				if (millisecondsSinceEpoch < Min)
					Min = millisecondsSinceEpoch.Value;
			}
			NumValues++;
		}

		public void FillColumnStatistics(ColumnStatistics columnStatistics)
		{
			if(columnStatistics.TimestampStatistics==null)
			{
				columnStatistics.TimestampStatistics = new TimestampStatistics
				{
					Minimum = Min,
					Maximum = Max
				};
			}
			else
			{
				if (Min < columnStatistics.TimestampStatistics.Minimum)
					columnStatistics.TimestampStatistics.Minimum = Min;
				if (Max > columnStatistics.TimestampStatistics.Maximum)
					columnStatistics.TimestampStatistics.Maximum = Max;
			}

			columnStatistics.NumberOfValues += NumValues;
			if (HasNull)
				columnStatistics.HasNull = true;
		}
	}
}
