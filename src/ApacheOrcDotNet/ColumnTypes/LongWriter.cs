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
		readonly OrcCompressedBuffer _presentBuffer;
		readonly OrcCompressedBuffer _dataBuffer;

		public LongWriter(bool isNullable, bool shouldAlignEncodedValues, OrcCompressedBufferFactory bufferFactory)
			: base(bufferFactory)
		{
			_isNullable = isNullable;
			_shouldAlignEncodedValues = shouldAlignEncodedValues;
			_bufferFactory = bufferFactory;

			if (_isNullable)
			{
				_presentBuffer = _bufferFactory.CreateBuffer(StreamKind.Present);
				_presentBuffer.MustBeIncluded = false;           //If we never have nulls, we won't write this stream
			}
			_dataBuffer = _bufferFactory.CreateBuffer(StreamKind.Data);
		}

		protected override ColumnEncodingKind DetectEncodingKind(IList<long?> values)
		{
			return ColumnEncodingKind.DirectV2;
		}

		protected override void AddDataStreamBuffers(IList<OrcCompressedBuffer> buffers, ColumnEncodingKind encodingKind)
		{
			if (encodingKind != ColumnEncodingKind.DirectV2)
				throw new NotSupportedException($"Only DirectV2 encoding is supported for {nameof(LongWriter)}");

			if (_isNullable)
				buffers.Add(_presentBuffer);
			buffers.Add(_dataBuffer);
		}

		protected override IStatistics CreateStatistics() => new LongWriterStatistics();

		protected override void EncodeValues(IList<long?> values, IList<OrcCompressedBuffer> buffers, IStatistics statistics)
		{
			var stats = (LongWriterStatistics)statistics;

			var valList = new List<long>(values.Count);

			int bufferIndex = 0;
			if (_isNullable)
			{
				var presentList = new List<bool>(values.Count);

				foreach (var value in values)
				{
					stats.AddValue(value);
					if (value.HasValue)
						valList.Add(value.Value);
					presentList.Add(value.HasValue);
				}

				var presentEncoder = new BitWriter(_presentBuffer);
				presentEncoder.Write(presentList);
				if (stats.HasNull)
					buffers[bufferIndex].MustBeIncluded = true;     //A null occurred.  Make sure to write this stream
				bufferIndex++;
			}
			else
			{
				foreach (var value in values)
				{
					stats.AddValue(value);
					valList.Add(value.Value);
				}
			}

			var valEncoder = new IntegerRunLengthEncodingV2Writer(_dataBuffer);
			valEncoder.Write(valList, true, _shouldAlignEncodedValues);
		}
	}
}
