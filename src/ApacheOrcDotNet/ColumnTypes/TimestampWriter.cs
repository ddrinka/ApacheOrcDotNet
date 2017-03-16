using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
	public class TimestampWriter : ColumnWriter<DateTime?>
	{
		readonly static DateTime _orcEpoch = new DateTime(2015, 1, 1, 0, 0, 0, DateTimeKind.Utc);
		readonly static DateTime _unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

		readonly bool _isNullable;
		readonly bool _shouldAlignEncodedValues;
		readonly OrcCompressedBuffer _presentBuffer;
		readonly OrcCompressedBuffer _dataBuffer;
		readonly OrcCompressedBuffer _secondaryBuffer;

		public TimestampWriter(bool isNullable, bool shouldAlignEncodedValues, OrcCompressedBufferFactory bufferFactory, uint columnId)
			:base(bufferFactory, columnId)
		{
			_isNullable = isNullable;
			_shouldAlignEncodedValues = shouldAlignEncodedValues;

			if (_isNullable)
			{
				_presentBuffer = bufferFactory.CreateBuffer(StreamKind.Present);
				_presentBuffer.MustBeIncluded = false;
			}
			_dataBuffer = bufferFactory.CreateBuffer(StreamKind.Data);
			_secondaryBuffer = bufferFactory.CreateBuffer(StreamKind.Secondary);
		}

		protected override ColumnEncodingKind DetectEncodingKind(IList<DateTime?> values)
		{
			return ColumnEncodingKind.DirectV2;
		}

		protected override void AddDataStreamBuffers(IList<OrcCompressedBuffer> buffers, ColumnEncodingKind encodingKind)
		{
			if (encodingKind != ColumnEncodingKind.DirectV2)
				throw new NotSupportedException($"Only DirectV2 encoding is supported for {nameof(TimestampWriter)}");

			if (_isNullable)
				buffers.Add(_presentBuffer);
			buffers.Add(_dataBuffer);
			buffers.Add(_secondaryBuffer);
		}

		protected override IStatistics CreateStatistics() => new TimestampWriterStatistics();

		protected override void EncodeValues(IList<DateTime?> values, ColumnEncodingKind encodingKind, IStatistics statistics)
		{
			var stats = (TimestampWriterStatistics)statistics;

			var secondsList = new List<long>(values.Count);
			var fractionsList = new List<long>(values.Count);

			if (_isNullable)
			{
				var presentList = new List<bool>(values.Count);

				foreach(var value in values)
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

					var presentEncoder = new BitWriter(_presentBuffer);
					presentEncoder.Write(presentList);
					if (stats.HasNull)
						_presentBuffer.MustBeIncluded = true;
				}
			}
			else
			{
				foreach(var value in values)
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
