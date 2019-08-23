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
	public class DecimalWriter : IColumnWriter<decimal?>
	{
		readonly bool _isNullable;
		readonly bool _shouldAlignEncodedValues;
        readonly int _precision;
		readonly int _scale;
		readonly OrcCompressedBuffer _presentBuffer;
		readonly OrcCompressedBuffer _dataBuffer;
		readonly OrcCompressedBuffer _secondaryBuffer;

		public DecimalWriter(bool isNullable, bool shouldAlignEncodedValues, int precision, int scale, OrcCompressedBufferFactory bufferFactory, uint columnId)
		{
			_isNullable = isNullable;
			_shouldAlignEncodedValues = shouldAlignEncodedValues;
            _precision = precision;
			_scale = scale;
			ColumnId = columnId;

			if (_precision > 18)
				throw new NotSupportedException("This implementation of DecimalWriter does not support precision greater than 18 digits (2^63)");

			if (_isNullable)
			{
				_presentBuffer = bufferFactory.CreateBuffer(StreamKind.Present);
				_presentBuffer.MustBeIncluded = false;
			}
			_dataBuffer = bufferFactory.CreateBuffer(StreamKind.Data);
			_secondaryBuffer = bufferFactory.CreateBuffer(StreamKind.Secondary);
		}

		public List<IStatistics> Statistics { get; } = new List<IStatistics>();
		public long CompressedLength => Buffers.Sum(s => s.Length);
		public uint ColumnId { get; }
        public OrcCompressedBuffer[] Buffers => _isNullable ? new[] { _presentBuffer, _dataBuffer, _secondaryBuffer } : new[] { _dataBuffer, _secondaryBuffer };
		public ColumnEncodingKind ColumnEncoding => ColumnEncodingKind.DirectV2;

		public void FlushBuffers()
		{
			foreach (var buffer in Buffers)
				buffer.Flush();
		}

		public void Reset()
		{
			foreach (var buffer in Buffers)
				buffer.Reset();
			if (_isNullable)
				_presentBuffer.MustBeIncluded = false;
			Statistics.Clear();
		}

		public void AddBlock(IList<decimal?> values)
		{
			var stats = new DecimalWriterStatistics();
			Statistics.Add(stats);
            if (_isNullable)
                _presentBuffer.AnnotatePosition(stats, rleValuesToConsume: 0, bitsToConsume: 0);
            _dataBuffer.AnnotatePosition(stats);
            _secondaryBuffer.AnnotatePosition(stats, rleValuesToConsume: 0);

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
						var rescaled = longAndScale.Rescale(_scale, truncateIfNecessary: false);
                        rescaled.Item1.CheckPrecision(_precision);
						wholePartsList.Add(rescaled.Item1);
						scaleList.Add(rescaled.Item2);
					}
					presentList.Add(value.HasValue);
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
					var rescaled = longAndScale.Rescale(_scale, truncateIfNecessary: false);
                    rescaled.Item1.CheckPrecision(_precision);
					wholePartsList.Add(rescaled.Item1);
					scaleList.Add(rescaled.Item2);
				}
			}

			var varIntEncoder = new VarIntWriter(_dataBuffer);
			varIntEncoder.Write(wholePartsList);

			var scaleEncoder = new IntegerRunLengthEncodingV2Writer(_secondaryBuffer);
			scaleEncoder.Write(scaleList, true, _shouldAlignEncodedValues);
		}
	}
}