using ApacheOrcDotNet.Statistics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
	public class LongWriterStatistics : ColumnWriterStatistics, IStatistics
	{
		public long? Min { get; set; }
		public long? Max { get; set; }
		public long? Sum { get; set; } = 0;
		public ulong NumValues { get; set; } = 0;
		public bool HasNull { get; set; } = false;

		public void AddValue(long? value)
		{
			if (!value.HasValue)
				HasNull = true;
			else
			{
				if (!Min.HasValue || value.Value < Min.Value)
					Min = value.Value;
				if (!Max.HasValue || value.Value > Max.Value)
					Max = value.Value;
				Sum = CheckedAdd(Sum, value.Value);
				NumValues++;
			}
		}

		public void FillColumnStatistics(ColumnStatistics columnStatistics)
		{
			if (columnStatistics.IntStatistics == null)
				columnStatistics.IntStatistics = new IntegerStatistics { Sum = 0 };

			var ds = columnStatistics.IntStatistics;

			if (Min.HasValue)
			{
				if (!ds.Minimum.HasValue || Min.Value < ds.Minimum.Value)
					ds.Minimum = Min.Value;
			}

			if (Max.HasValue)
			{
				if (!ds.Maximum.HasValue || Max.Value > ds.Maximum.Value)
					ds.Maximum = Max.Value;
			}

			ds.Sum = CheckedAdd(ds.Sum, Sum);

			columnStatistics.NumberOfValues += NumValues;
			if (HasNull)
				columnStatistics.HasNull = true;
		}

		long? CheckedAdd(long? left, long? right)
		{
			if (!left.HasValue || !right.HasValue)
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
