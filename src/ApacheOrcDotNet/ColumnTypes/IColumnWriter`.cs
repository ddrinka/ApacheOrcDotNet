using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
    public interface IColumnWriter<T> : IColumnWriter
    {
		/// <summary>
		/// Write a full index stride's worth of data.  This should be called repeatedly until the sum of all buffers in all columns is greater than the stripe size
		/// </summary>
		/// <param name="values">A block of values to process</param>
		void AddBlock(IList<T> values);
	}
}
