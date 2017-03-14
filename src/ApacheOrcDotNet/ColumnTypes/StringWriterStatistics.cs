using ApacheOrcDotNet.Statistics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class StringWriterStatistics : ColumnWriterStatistics, IStatistics
    {
		public string Min { get; set; } = null;
		public string Max { get; set; } = null;
		public long Sum { get; set; } = 0;
		public ulong NumValues { get; set; } = 0;
		public bool HasNull { get; set; } = false;

		public void AddValue(string value)
		{
			if (value == null)
				HasNull = true;
			else
			{
				if (Max == null || string.Compare(value, Max, StringComparison.Ordinal) > 0)
					Max = value;
				if (Min == null || string.Compare(value, Min, StringComparison.Ordinal) < 0)
					Min = value;
				Sum += value.Length;
			}
			NumValues++;
		}

		public void FillColumnStatistics(ColumnStatistics columnStatistics)
		{
			if (columnStatistics.StringStatistics == null)
			{
				columnStatistics.StringStatistics = new StringStatistics
				{
					Minimum = Min,
					Maximum = Max,
					Sum = Sum
				};
			}
			else
			{
				if (string.Compare(Min, columnStatistics.StringStatistics.Minimum, StringComparison.Ordinal) < 0)
					columnStatistics.StringStatistics.Minimum = Min;
				if (string.Compare(Max, columnStatistics.StringStatistics.Maximum, StringComparison.Ordinal) > 0)
					columnStatistics.StringStatistics.Maximum = Max;
				columnStatistics.StringStatistics.Sum += Sum;
			}

			columnStatistics.NumberOfValues += NumValues;
			if (HasNull)
				columnStatistics.HasNull = true;
		}
	}
}
