using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class DoubleWriter : ColumnWriter<double?>
    {
		readonly bool _isNullable;
		readonly OrcCompressedBuffer _presentBuffer;
		readonly OrcCompressedBuffer _dataBuffer;

		public DoubleWriter(bool isNullable, OrcCompressedBufferFactory bufferFactory, uint columnId)
			: base(bufferFactory, columnId)
		{
			_isNullable = isNullable;

			if (_isNullable)
			{
				_presentBuffer = bufferFactory.CreateBuffer(StreamKind.Present);
				_presentBuffer.MustBeIncluded = false;
			}
			_dataBuffer = bufferFactory.CreateBuffer(StreamKind.Data);
		}

		protected override ColumnEncodingKind DetectEncodingKind(IList<double?> values)
		{
			return ColumnEncodingKind.Direct;
		}

		protected override void AddDataStreamBuffers(IList<OrcCompressedBuffer> buffers, ColumnEncodingKind encodingKind)
		{
			if (encodingKind != ColumnEncodingKind.Direct)
				throw new NotSupportedException($"Only Direct encoding is supported for {nameof(DoubleWriter)}");

			if (_isNullable)
				buffers.Add(_presentBuffer);
			buffers.Add(_dataBuffer);
		}

		protected override IStatistics CreateStatistics() => new DoubleWriterStatistics();

		protected override void EncodeValues(IList<double?> values, IList<OrcCompressedBuffer> buffers, IStatistics statistics)
		{
			var stats = (DoubleWriterStatistics)statistics;

			var valList = new List<double>(values.Count);

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
					_presentBuffer.MustBeIncluded = true;
			}
			else
			{
				foreach(var value in values)
				{
					stats.AddValue(value);
					valList.Add(value.Value);
				}
			}

			foreach(var value in valList)
			{
				_dataBuffer.WriteDoubleBE(value);
			}
		}
	}
}
