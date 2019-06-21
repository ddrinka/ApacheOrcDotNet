using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
	public class DateWriter : IColumnWriter<DateTime?>
	{
		readonly static DateTime _unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0);		//Here we'll use a Kind=Unspecified DateTime to avoid muddling the subtraction below

		readonly bool _isNullable;
		readonly bool _shouldAlignEncodedValues;
		readonly OrcCompressedBuffer _presentBuffer;
		readonly OrcCompressedBuffer _dataBuffer;

		public DateWriter(bool isNullable, bool shouldAlignEncodedValues, OrcCompressedBufferFactory bufferFactory, uint columnId)
		{
			_isNullable = isNullable;
			_shouldAlignEncodedValues = shouldAlignEncodedValues;
			ColumnId = columnId;

			if (_isNullable)
			{
				_presentBuffer = bufferFactory.CreateBuffer(StreamKind.Present);
				_presentBuffer.MustBeIncluded = false;
			}
			_dataBuffer = bufferFactory.CreateBuffer(StreamKind.Data);
		}

		public List<IStatistics> Statistics { get; } = new List<IStatistics>();
		public long CompressedLength => Buffers.Sum(s => s.Length);
		public uint ColumnId { get; }
		public OrcCompressedBuffer[] Buffers => _isNullable ? new[] { _presentBuffer, _dataBuffer } : new[] { _dataBuffer };
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

		public void AddBlock(IList<DateTime?> values)
		{
			var stats = new DateWriterStatistics();
			Statistics.Add(stats);
            if (_isNullable)
                _presentBuffer.AnnotatePosition(stats, rleValuesToConsume: 0, bitsToConsume: 0);
            _dataBuffer.AnnotatePosition(stats, rleValuesToConsume: 0);

			var datesList = new List<long>(values.Count);

            if (_isNullable)
            {
                var presentList = new List<bool>(values.Count);

                foreach (var value in values)
                {
                    if (!value.HasValue)
                    {
                        stats.AddValue(null);
                        presentList.Add(false);
                    }
                    else
                    {
                        var daysSinceEpoch = (int)(value.Value - _unixEpoch).TotalDays;
                        stats.AddValue(daysSinceEpoch);
                        presentList.Add(true);
                        datesList.Add(daysSinceEpoch);
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
                    var daysSinceEpoch = (int)(value.Value - _unixEpoch).TotalDays;
                    stats.AddValue(daysSinceEpoch);
                    datesList.Add(daysSinceEpoch);
                }
            }

			var datesEncoder = new IntegerRunLengthEncodingV2Writer(_dataBuffer);
			datesEncoder.Write(datesList, true, _shouldAlignEncodedValues);
		}
	}
}