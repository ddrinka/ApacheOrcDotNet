using ApacheOrcDotNet.Statistics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class DoubleWriterStatistics : ColumnWriterStatistics, IStatistics
    {
		public double? Min { get; set; }
		public double? Max { get; set; }
		public double? Sum { get; set; } = 0;
		public ulong NumValues { get; set; } = 0;
		public bool HasNull { get; set; } = false;

		public void AddValue(double? value)
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
			if (columnStatistics.DoubleStatistics == null)
				columnStatistics.DoubleStatistics = new DoubleStatistics { Sum = 0 };

			var ds = columnStatistics.DoubleStatistics;

			if(Min.HasValue)
			{
				if (!ds.Minimum.HasValue || Min.Value < ds.Minimum.Value)
					ds.Minimum = Min.Value;
			}

			if(Max.HasValue)
			{
				if (!ds.Maximum.HasValue || Max.Value > ds.Maximum.Value)
					ds.Maximum = Max.Value;
			}

			ds.Sum = CheckedAdd(ds.Sum, Sum);

			columnStatistics.NumberOfValues += NumValues;
			if (HasNull)
				columnStatistics.HasNull = true;
		}

		double? CheckedAdd(double? left, double? right)
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
