using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
	public class TimestampWriter : IColumnWriter<DateTime?>
	{
		readonly static DateTime _orcEpoch = new DateTime(2015, 1, 1, 0, 0, 0, DateTimeKind.Utc);
		readonly static DateTime _unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

		readonly bool _isNullable;
		readonly bool _shouldAlignEncodedValues;
		readonly OrcCompressedBuffer _presentBuffer;
		readonly OrcCompressedBuffer _dataBuffer;
		readonly OrcCompressedBuffer _secondaryBuffer;

		public TimestampWriter(bool isNullable, bool shouldAlignEncodedValues, OrcCompressedBufferFactory bufferFactory, uint columnId)
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

		public void AddBlock(IList<DateTime?> values)
		{
			var stats = new TimestampWriterStatistics();
			Statistics.Add(stats);
            if (_isNullable)
                _presentBuffer.AnnotatePosition(stats, rleValuesToConsume: 0, bitsToConsume: 0);
            _dataBuffer.AnnotatePosition(stats, rleValuesToConsume: 0);
            _secondaryBuffer.AnnotatePosition(stats, rleValuesToConsume: 0);

			var secondsList = new List<long>(values.Count);
			var fractionsList = new List<long>(values.Count);

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
                        long millisecondsSinceUnixEpoch;
                        long fraction;
                        var seconds = GetValues(value.Value, out millisecondsSinceUnixEpoch, out fraction);
                        stats.AddValue(millisecondsSinceUnixEpoch);
                        presentList.Add(true);
                        secondsList.Add(seconds);
                        fractionsList.Add(fraction);
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
                    long millisecondsSinceUnixEpoch;
                    long fraction;
                    var seconds = GetValues(value.Value, out millisecondsSinceUnixEpoch, out fraction);
                    stats.AddValue(millisecondsSinceUnixEpoch);
                    secondsList.Add(seconds);
                    fractionsList.Add(fraction);
                }
            }

			var secondsEncoder = new IntegerRunLengthEncodingV2Writer(_dataBuffer);
			secondsEncoder.Write(secondsList, true, _shouldAlignEncodedValues);

			var fractionsEncoder = new IntegerRunLengthEncodingV2Writer(_secondaryBuffer);
			fractionsEncoder.Write(fractionsList, false, _shouldAlignEncodedValues);
		}

		long GetValues(DateTime dateTime, out long millisecondsSinceUnixEpoch, out long fraction)
		{
			if (dateTime.Kind != DateTimeKind.Utc)
				throw new NotSupportedException("Only UTC DateTimes are supported in Timestamp columns");

			var ticks = (dateTime - _orcEpoch).Ticks;
			var seconds = ticks / TimeSpan.TicksPerSecond;
			millisecondsSinceUnixEpoch = (dateTime - _unixEpoch).Ticks / TimeSpan.TicksPerMillisecond;
			var remainderTicks = (int)(ticks - (seconds * TimeSpan.TicksPerSecond));
			var nanoseconds = Math.Abs(remainderTicks) * 100;
			int scaledNanoseconds;
			var scale = RemoveZeros(nanoseconds, out scaledNanoseconds);
			fraction = (scaledNanoseconds << 3) | scale;
			return seconds;
		}

		byte RemoveZeros(int nanoseconds, out int scaledNanoseconds)
		{
			if (nanoseconds >= 1 * 1000 * 1000 * 1000)
				throw new ArgumentException("Nanoseconds must be less than a single second");
			scaledNanoseconds = nanoseconds / (100 * 1000 * 1000);
			if (scaledNanoseconds * 100 * 1000 * 1000 == nanoseconds)
				return 7;
			scaledNanoseconds = nanoseconds / (10 * 1000 * 1000);
			if (scaledNanoseconds * 10 * 1000 * 1000 == nanoseconds)
				return 6;
			scaledNanoseconds = nanoseconds / (1 * 1000 * 1000);
			if (scaledNanoseconds * 1 * 1000 * 1000 == nanoseconds)
				return 5;
			scaledNanoseconds = nanoseconds / (100 * 1000);
			if (scaledNanoseconds * 100 * 1000 == nanoseconds)
				return 4;
			scaledNanoseconds = nanoseconds / (10 * 1000);
			if (scaledNanoseconds * 10 * 1000 == nanoseconds)
				return 3;
			scaledNanoseconds = nanoseconds / (1 * 1000);
			if (scaledNanoseconds * 1 * 1000 == nanoseconds)
				return 2;
			scaledNanoseconds = nanoseconds / (100);
			if (scaledNanoseconds * 100 == nanoseconds)
				return 1;
			scaledNanoseconds = nanoseconds;
			return 0;
		}
	}
}