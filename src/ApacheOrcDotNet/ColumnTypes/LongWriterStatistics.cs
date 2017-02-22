using ApacheOrcDotNet.Statistics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
	public class LongWriterStatistics : ColumnWriterStatistics, IStatistics
	{
		public long Min { get; set; } = long.MaxValue;
		public long Max { get; set; } = long.MinValue;
		public long? Sum { get; set; } = 0;
		public ulong NumValues { get; set; } = 0;
		public bool HasNull { get; set; } = false;

		public void AddValue(long? value)
		{
			if (!value.HasValue)
				HasNull = true;
			else
			{
				if (value > Max)
					Max = value.Value;
				if (value < Min)
					Min = value.Value;
				Sum = CheckedAdd(Sum, value.Value);
			}
			NumValues++;
		}

		public void FillColumnStatistics(ColumnStatistics columnStatistics)
		{
			if (columnStatistics.IntStatistics == null)
			{
				columnStatistics.IntStatistics = new IntegerStatistics
				{
					Mimumum = Min,
					Maximum = Max,
					Sum = Sum
				};
			}
			else
			{
				if (Min < columnStatistics.IntStatistics.Mimumum)
					columnStatistics.IntStatistics.Mimumum = Min;
				if (Max > columnStatistics.IntStatistics.Maximum)
					columnStatistics.IntStatistics.Maximum = Max;
				if (!Sum.HasValue)
					columnStatistics.IntStatistics.Sum = null;
				else
					columnStatistics.IntStatistics.Sum = CheckedAdd(columnStatistics.IntStatistics.Sum, Sum.Value);
			}

			columnStatistics.NumberOfValues += NumValues;
			if (HasNull)
				columnStatistics.HasNull = true;
		}

		long? CheckedAdd(long? left, long right)
		{
			if (!left.HasValue)
				return null;

			try
			{
				checked
				{
					return left.Value + right;
				}
			}
			catch (OverflowException)
			{
				return null;
			}
		}
	}
}
