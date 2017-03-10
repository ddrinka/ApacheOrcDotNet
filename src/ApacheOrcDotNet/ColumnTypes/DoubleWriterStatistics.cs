using ApacheOrcDotNet.Statistics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class DoubleWriterStatistics : ColumnWriterStatistics, IStatistics
    {
		public double Min { get; set; } = double.MaxValue;
		public double Max { get; set; } = double.MinValue;
		public double? Sum { get; set; } = 0;
		public ulong NumValues { get; set; } = 0;
		public bool HasNull { get; set; } = false;

		public void AddValue(double? value)
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
			if(columnStatistics.DoubleStatistics==null)
			{
				columnStatistics.DoubleStatistics = new DoubleStatistics
				{
					Minimum = Min,
					Maximum = Max,
					Sum = Sum
				};
			}
			else
			{
				if (Min < columnStatistics.DoubleStatistics.Minimum)
					columnStatistics.DoubleStatistics.Minimum = Min;
				if (Max > columnStatistics.DoubleStatistics.Maximum)
					columnStatistics.DoubleStatistics.Maximum = Max;
				if (!Sum.HasValue)
					columnStatistics.DoubleStatistics.Sum = null;
				else
					columnStatistics.DoubleStatistics.Sum = CheckedAdd(columnStatistics.DoubleStatistics.Sum, Sum.Value);
			}

			columnStatistics.NumberOfValues += NumValues;
			if (HasNull)
				columnStatistics.HasNull = true;
		}

		double? CheckedAdd(double? left, double right)
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
