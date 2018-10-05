using ApacheOrcDotNet.Statistics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class DecimalWriterStatistics : ColumnWriterStatistics, IStatistics
    {
		public decimal? Min { get; set; }
		public decimal? Max { get; set; }
		public decimal? Sum { get; set; } = 0;
		public ulong NumValues { get; set; } = 0;
		public bool HasNull { get; set; } = false;

		public void AddValue(decimal? value)
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
			if (columnStatistics.DecimalStatistics == null)
				columnStatistics.DecimalStatistics = new DecimalStatistics() { Sum = "0" };		//null means overflow so start with zero

			var ds = columnStatistics.DecimalStatistics;

			if (Min.HasValue)
			{
				if (String.IsNullOrEmpty(ds.Minimum) || Min.Value < Decimal.Parse(ds.Minimum))
					ds.Minimum = Min.Value.ToString();
			}

			if(Max.HasValue)
				if (String.IsNullOrEmpty(ds.Maximum) || Max.Value > Decimal.Parse(ds.Maximum))
					ds.Maximum = Max.Value.ToString();


			if (!String.IsNullOrEmpty(ds.Sum))
				ds.Sum = CheckedAdd(decimal.Parse(ds.Sum), Sum.Value)?.ToString();

			columnStatistics.NumberOfValues += NumValues;
			if (HasNull)
				columnStatistics.HasNull = true;
		}

		decimal? CheckedAdd(decimal? left, decimal? right)
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
			catch(OverflowException)
			{
				return null;
			}
		}
	}
}
