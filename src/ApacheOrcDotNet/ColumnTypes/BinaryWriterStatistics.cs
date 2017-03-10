using ApacheOrcDotNet.Statistics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class BinaryWriterStatistics : ColumnWriterStatistics, IStatistics
    {
		public long Sum { get; set; }
		public ulong NumValues { get; set; } = 0;
		public bool HasNull { get; set; } = false;

		public void AddValue(byte[] data)
		{
			if (data == null)
				HasNull = true;
			else
			{
				Sum += data.Length;
			}
			NumValues++;
		}

		public void FillColumnStatistics(ColumnStatistics columnStatistics)
		{
			if(columnStatistics.BinaryStatistics==null)
			{
				columnStatistics.BinaryStatistics = new BinaryStatistics
				{
					Sum = Sum
				};
			}
			else
			{
				columnStatistics.BinaryStatistics.Sum += Sum;
			}

			columnStatistics.NumberOfValues += NumValues;
			if (HasNull)
				columnStatistics.HasNull = true;
		}
	}
}
