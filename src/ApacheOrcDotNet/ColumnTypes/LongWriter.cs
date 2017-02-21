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

		public LongWriter(bool isNullable, bool shouldAlignEncodedValues, OrcCompressedBufferFactory bufferFactory)
			: base(bufferFactory)
		{
			_isNullable = isNullable;
			_shouldAlignEncodedValues = shouldAlignEncodedValues;
		}

		protected override int NumDataStreams => _isNullable ? 2 : 1;
		protected override IStatistics CreateStatistics() => new LongWriterStatistics();

		protected override void EncodeValues(IList<long?> values, OrcCompressedBuffer[] buffers, IStatistics statistics)
		{
			var stats = (LongWriterStatistics)statistics;

			var valList = new List<long>(values.Count);
			var presentList = new List<bool>(values.Count);

			foreach (var value in values)
			{
				stats.AddValue(value);
				if (value.HasValue)
					valList.Add(value.Value);
				presentList.Add(value.HasValue);
			}

			int bufferIndex = 0;
			if (presentList.Count != 0)
			{
				if (!_isNullable)
					throw new InvalidOperationException($"Null values were present in a non-nullable {nameof(LongWriter)} column");

				var presentEncoder = new BitWriter(buffers[bufferIndex++]);
				presentEncoder.Write(presentList);
			}

			var valEncoder = new IntegerRunLengthEncodingV2Writer(buffers[bufferIndex]);
			valEncoder.Write(valList, true, _shouldAlignEncodedValues);
		}
	}
}
