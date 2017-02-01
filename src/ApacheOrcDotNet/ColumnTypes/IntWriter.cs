using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Statistics;

namespace ApacheOrcDotNet.ColumnTypes
{
	public class IntWriter : ColumnWriter<int>
	{
		protected override int NumDataStreams => 1;
		protected override IStatistics<int> CreateStatistics() => new IntWriterStatistics();

		protected override void EncodeValues(ArraySegment<int> values, OrcCompressedBuffer[] buffers, IStatistics<int> statistics)
		{
			throw new NotImplementedException();
		}
	}

	public class IntWriterStatistics : ColumnWriterStatistics, IStatistics<int>
	{
		public IntegerStatistics IntegerStatistics { get; } = new IntegerStatistics();
	}
}
