using ApacheOrcDotNet.Statistics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class DateWriterStatistics : ColumnWriterStatistics, IStatistics
    {
		public int Min { get; set; } = int.MaxValue;
		public int Max { get; set; } = int.MinValue;
		public ulong NumValues { get; set; } = 0;
		public bool HasNull { get; set; } = false;

		public void AddValue(int? date)
		{
			if (!date.HasValue)
				HasNull = true;
			else
			{
				if (date > Max)
					Max = date.Value;
				if (date < Min)
					Min = date.Value;
			}
			NumValues++;
		}

		public void FillColumnStatistics(ColumnStatistics columnStatistics)
		{
			if(columnStatistics.DateStatistics == null)
			{
				columnStatistics.DateStatistics = new DateStatistics
				{
					Minimum = Min,
					Maximum = Max
				};
			}
			else
			{
				if (Min < columnStatistics.DateStatistics.Minimum)
					columnStatistics.DateStatistics.Minimum = Min;
				if (Max > columnStatistics.DateStatistics.Maximum)
					columnStatistics.DateStatistics.Maximum = Max;
			}

			columnStatistics.NumberOfValues += NumValues;
			if (HasNull)
				columnStatistics.HasNull = true;
		}
	}
}
