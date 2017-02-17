using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Statistics;
using ApacheOrcDotNet.Encodings;

namespace ApacheOrcDotNet.ColumnTypes
{
	public class LongWriter : ColumnWriter<long?>
	{
		readonly bool _isNullable;
		readonly bool _shouldAlignEncodedValues;

		public LongWriter(bool isNullable, bool shouldAlignEncodedValues)
		{
			_isNullable = isNullable;
			_shouldAlignEncodedValues = shouldAlignEncodedValues;
		}

		protected override int NumDataStreams => _isNullable ? 2 : 1;
		protected override IStatistics CreateStatistics() => new LongWriterStatistics();

		protected override void EncodeValues(ArraySegment<long?> values, OrcCompressedBuffer[] buffers, IStatistics statistics)
		{
			var stats = (LongWriterStatistics)statistics;

			foreach (var value in values)		//TODO benchmark this to see if LINQ Sum/Min/Max is faster than a function call per value
				stats.AddValue(value);

			var encoder = new IntegerRunLengthEncodingV2Writer(buffers[0]);
			//encoder.Write(values, true, _shouldAlignEncodedValues);
		}
	}
}
