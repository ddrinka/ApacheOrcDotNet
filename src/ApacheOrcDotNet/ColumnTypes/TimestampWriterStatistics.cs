using ApacheOrcDotNet.Statistics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class TimestampWriterStatistics : ColumnWriterStatistics, IStatistics
    {
		public long? Min { get; set; }
		public long? Max { get; set; }
		public ulong NumValues { get; set; } = 0;
		public bool HasNull { get; set; } = false;

		public void AddValue(long? millisecondsSinceUnixEpoch)
		{
			if (!millisecondsSinceUnixEpoch.HasValue)
				HasNull = true;
			else
			{
				if (!Min.HasValue || millisecondsSinceUnixEpoch.Value < Min.Value)
					Min = millisecondsSinceUnixEpoch.Value;

				if (!Max.HasValue || millisecondsSinceUnixEpoch.Value > Max.Value)
					Max = millisecondsSinceUnixEpoch.Value;
				NumValues++;
			}
		}

		public void FillColumnStatistics(ColumnStatistics columnStatistics)
		{
			if (columnStatistics.TimestampStatistics == null)
				columnStatistics.TimestampStatistics = new TimestampStatistics();

			var ds = columnStatistics.TimestampStatistics;

			if (Min.HasValue)
			{
				if (!ds.Minimum.HasValue || Min.Value < ds.Minimum.Value)
					ds.Minimum = Min.Value;
			}

			if (Max.HasValue)
			{
				if (!ds.Maximum.HasValue || Max.Value > ds.Maximum)
					ds.Maximum = Max.Value;
			}

			columnStatistics.NumberOfValues += NumValues;
			if (HasNull)
				columnStatistics.HasNull = true;
		}
	}
}
