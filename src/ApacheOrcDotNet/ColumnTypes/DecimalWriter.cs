using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
	public class DecimalWriter : ColumnWriter<decimal?>
	{
		readonly bool _isNullable;
		readonly bool _shouldAlignEncodedValues;
		readonly int _scale;
		readonly OrcCompressedBuffer _presentBuffer;
		readonly OrcCompressedBuffer _dataBuffer;
		readonly OrcCompressedBuffer _secondaryBuffer;

		public DecimalWriter(bool isNullable, bool shouldAlignEncodedValues, int precision, int scale, OrcCompressedBufferFactory bufferFactory, uint columnId)
			: base(bufferFactory, columnId)
		{
			_isNullable = isNullable;
			_shouldAlignEncodedValues = shouldAlignEncodedValues;
			_scale = scale;

			if (precision > 18)
				throw new NotSupportedException("This implementation of DecimalWriter does not support precision greater than 18 digits (2^63)");

			if (_isNullable)
			{
				_presentBuffer = bufferFactory.CreateBuffer(StreamKind.Present);
				_presentBuffer.MustBeIncluded = false;
			}
			_dataBuffer = bufferFactory.CreateBuffer(StreamKind.Data);
			_secondaryBuffer = bufferFactory.CreateBuffer(StreamKind.Secondary);
		}

		protected override ColumnEncodingKind DetectEncodingKind(IList<decimal?> values)
		{
			return ColumnEncodingKind.DirectV2;
		}

		protected override void AddDataStreamBuffers(IList<OrcCompressedBuffer> buffers, ColumnEncodingKind encodingKind)
		{
			if (encodingKind != ColumnEncodingKind.DirectV2)
				throw new NotSupportedException($"Only DirectV2 encoding is supported for {nameof(DecimalWriter)}");

			if (_isNullable)
				buffers.Add(_presentBuffer);
			buffers.Add(_dataBuffer);
			buffers.Add(_secondaryBuffer);
		}

		protected override IStatistics CreateStatistics() => new DecimalWriterStatistics();

		protected override void EncodeValues(IList<decimal?> values, ColumnEncodingKind encodingKind, IStatistics statistics)
		{
			var stats = (DecimalWriterStatistics)statistics;

			var wholePartsList = new List<long>(values.Count);
			var scaleList = new List<long>(values.Count);

			if (_isNullable)
			{
				var presentList = new List<bool>(values.Count);

				foreach (var value in values)
				{
					stats.AddValue(value);
					if (value.HasValue)
					{
						var longAndScale = value.Value.ToLongAndScale();
						//var rescaled = longAndScale.Rescale(_scale, false);
						wholePartsList.Add(longAndScale.Item1);
						scaleList.Add(longAndScale.Item2);
					}
				}

				var presentEncoder = new BitWriter(_presentBuffer);
				presentEncoder.Write(presentList);
				if (stats.HasNull)
					_presentBuffer.MustBeIncluded = true;
			}
			else
			{
				foreach (var value in values)
				{
					stats.AddValue(value);
					var longAndScale = value.Value.ToLongAndScale();
					//var rescaled = longAndScale.Rescale(_scale, false);
					wholePartsList.Add(longAndScale.Item1);
					scaleList.Add(longAndScale.Item2);
				}
			}

			var varIntEncoder = new VarIntWriter(_dataBuffer);
			varIntEncoder.Write(wholePartsList);

			var scaleEncoder = new IntegerRunLengthEncodingV2Writer(_secondaryBuffer);
			scaleEncoder.Write(scaleList, true, _shouldAlignEncodedValues);
		}
	}
}
