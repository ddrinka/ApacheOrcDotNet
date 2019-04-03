using ApacheOrcDotNet.Statistics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class BinaryWriterStatistics : ColumnWriterStatistics, IStatistics
    {
		public long? Sum { get; set; } = 0;
		public ulong NumValues { get; set; } = 0;
		public bool HasNull { get; set; } = false;

		public void AddValue(byte[] data)
		{
			if (data == null)
				HasNull = true;
			else
			{
				Sum = CheckedAdd(Sum, data.Length);
				NumValues++;
			}
		}

		public void FillColumnStatistics(ColumnStatistics columnStatistics)
		{
			if (columnStatistics.BinaryStatistics == null)
				columnStatistics.BinaryStatistics = new BinaryStatistics { Sum = 0 };

			var ds = columnStatistics.BinaryStatistics;

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
					return left.Value + right.Value;
				}
			}
			catch (OverflowException)
			{
				return null;
			}
		}
	}
}
