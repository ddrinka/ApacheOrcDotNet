using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Statistics;
using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.Protocol;

namespace ApacheOrcDotNet.ColumnTypes
{
	public class LongWriter : ColumnWriter<long?>
	{
		readonly bool _isNullable;
		readonly bool _shouldAlignEncodedValues;
		readonly OrcCompressedBufferFactory _bufferFactory;

		public LongWriter(bool isNullable, bool shouldAlignEncodedValues, OrcCompressedBufferFactory bufferFactory)
			: base(bufferFactory)
		{
			_isNullable = isNullable;
			_shouldAlignEncodedValues = shouldAlignEncodedValues;
			_bufferFactory = bufferFactory;
		}

		protected override ColumnEncodingKind DetectEncodingKind(IList<long?> values)
		{
			return ColumnEncodingKind.DirectV2;
		}

		protected override OrcCompressedBuffer[] CreateDataStreamBuffers(ColumnEncodingKind encodingKind)
		{
			if (encodingKind != ColumnEncodingKind.DirectV2)
				throw new NotSupportedException($"Only DirectV2 encoding is supported for {nameof(LongWriter)}");

			if (_isNullable)
			{
				var buffers = new OrcCompressedBuffer[2];
				buffers[0] = _bufferFactory.CreateBuffer(StreamKind.Present);
				buffers[1] = _bufferFactory.CreateBuffer(StreamKind.Data);
				return buffers;
			}
			else
			{
				var buffers = new OrcCompressedBuffer[1];
				buffers[0] = _bufferFactory.CreateBuffer(StreamKind.Data);
				return buffers;
			}
		}

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
