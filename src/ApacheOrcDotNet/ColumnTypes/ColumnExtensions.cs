using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Infrastructure;
using ApacheOrcDotNet.Statistics;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace ApacheOrcDotNet.ColumnTypes
{
	public static class ColumnExtensions
	{
		public static void WriteToBuffer(this IEnumerable<IStatistics> statistics, Stream outputStream, Func<int,bool> bufferIndexMustBeIncluded)
		{
			var indexes = new Protocol.RowIndex();
			foreach (var stats in statistics)
			{
				var indexEntry = new Protocol.RowIndexEntry();
				stats.FillPositionList(indexEntry.Positions, bufferIndexMustBeIncluded);
				stats.FillColumnStatistics(indexEntry.Statistics);
				indexes.Entry.Add(indexEntry);
			}

			StaticProtoBuf.Serializer.Serialize(outputStream, indexes);
		}

		public static void AddDataStream(this Protocol.StripeFooter footer, uint columnId, OrcCompressedBuffer buffer)
		{
			var stream = new Protocol.Stream
			{
				Column = columnId,
				Kind = buffer.StreamKind,
				Length = (ulong)buffer.Length
			};
			footer.Streams.Add(stream);
		}

		public static void AddColumn(this Protocol.StripeFooter footer, Protocol.ColumnEncodingKind columnEncodingKind, uint dictionarySize = 0)
		{
			var columnEncoding = new Protocol.ColumnEncoding
			{
				Kind = columnEncodingKind,
				DictionarySize = dictionarySize
			};
			footer.Columns.Add(columnEncoding);
		}
	}
}
