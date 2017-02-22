using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Statistics;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace ApacheOrcDotNet.ColumnTypes
{
	public abstract class ColumnWriter<T> : IColumnWriter
	{
		readonly OrcCompressedBufferFactory _bufferFactory;     //Only used for indexes
		bool _blockAddingIsComplete = false;
		OrcCompressedBuffer[] _stripeStreamBuffers = null;
		Protocol.ColumnEncodingKind _columnEncoding = Protocol.ColumnEncodingKind.DirectV2;

		public List<IStatistics> Statistics { get; } = new List<IStatistics>();
		public IList<long> CompressedLengths => _stripeStreamBuffers?.Select(s => s.Length).ToArray() ?? new long[] { 0 };

		protected abstract IStatistics CreateStatistics();
		protected abstract Protocol.ColumnEncodingKind DetectEncodingKind(IList<T> values);
		protected abstract OrcCompressedBuffer[] CreateDataStreamBuffers(Protocol.ColumnEncodingKind encodingKind);
		protected abstract void EncodeValues(IList<T> values, OrcCompressedBuffer[] buffers, IStatistics statistics);

		public ColumnWriter(OrcCompressedBufferFactory bufferFactory)
		{
			_bufferFactory = bufferFactory;
		}

		/// <summary>
		/// Write a full index stride's worth of data.  This should be called repeatedly until the sum of all buffers in all columns is greater than the stripe size
		/// </summary>
		/// <param name="values">A block of values to process</param>
		public void AddBlock(IList<T> values)
		{
			if (_blockAddingIsComplete)
				throw new InvalidOperationException("Attempted to add blocks after calling CompleteAddingBlocks");

			if (_stripeStreamBuffers == null)
			{
				_columnEncoding = DetectEncodingKind(values);
				_stripeStreamBuffers = CreateDataStreamBuffers(_columnEncoding);
			}

			var statistics = CreateStatistics();
			foreach (var buffer in _stripeStreamBuffers)
			{
				if (buffer.AreCompressing)
					statistics.AnnotatePosition(buffer.CompressedBuffer.Length, buffer.CurrentBlockLength, 0);      //Our implementation always ends the RLE at the stride
				else
					statistics.AnnotatePosition(buffer.CompressedBuffer.Length + buffer.CurrentBlockLength, 0);     //If we're not compressing, output the total length as a single value
			}

			EncodeValues(values, _stripeStreamBuffers, statistics);

			Statistics.Add(statistics);
		}

		/// <summary>
		/// Inform the writer that no further blocks will be delivered--Compress all buffered data and conclude processing.
		/// </summary>
		public void CompleteAddingBlocks()
		{
			_blockAddingIsComplete = true;
			foreach (var buffer in _stripeStreamBuffers)
				buffer.WritingCompleted();
		}

		public void CopyTo(Stream outputStream)
		{
			if (!_blockAddingIsComplete)
				throw new InvalidOperationException("Blocking adding not marked as complete");

			for (int i = 0; i < _stripeStreamBuffers.Length; i++)
				_stripeStreamBuffers[i].CopyTo(outputStream);
		}

		public void CopyStatisticsTo(Stream outputStream)
		{
			if (!_blockAddingIsComplete)
				throw new InvalidOperationException("Blocking adding not marked as complete");

			var indexes = new Protocol.RowIndex();
			foreach (var stats in Statistics)
			{
				var indexEntry = new Protocol.RowIndexEntry();
				stats.FillPositionList(indexEntry.Positions);
				stats.FillColumnStatistics(indexEntry.Statistics);
				indexes.Entry.Add(indexEntry);
			}

			var buffer = _bufferFactory.CreateBuffer(Protocol.StreamKind.RowIndex);
			ProtoBuf.Serializer.Serialize(buffer, indexes);
			buffer.WritingCompleted();
			buffer.CopyTo(outputStream);
		}

		public void FillStripeFooter(Protocol.StripeFooter footer)
		{
			if (!_blockAddingIsComplete)
				throw new InvalidOperationException("Blocking adding not marked as complete");

			var columnEncoding = new Protocol.ColumnEncoding
			{
				Kind = _columnEncoding,
				DictionarySize = 0      //TODO fill in dictionary size when we have a dictionary
			};
			footer.Columns.Add(columnEncoding);

			for (int i = 0; i < _stripeStreamBuffers.Length; i++)
			{
				var stream = new Protocol.Stream
				{
					Column = (uint)footer.Columns.Count - 1,
					Kind = _stripeStreamBuffers[i].StreamKind,
					Length = (ulong)_stripeStreamBuffers[i].CompressedBuffer.Length
				};
				footer.Streams.Add(stream);
			}
		}

		public void Reset()
		{
			_blockAddingIsComplete = false;
			_stripeStreamBuffers = null;
			Statistics.Clear();
		}
	}

	public class ColumnWriterStatistics
	{
		public List<long> CompressedBufferOffsets { get; } = new List<long>();
		public List<long> DecompressedOffsets { get; } = new List<long>();
		public List<long> RleValuesToConsume { get; } = new List<long>();

		public void AnnotatePosition(long compressedBufferOffset, long decompressedOffset, long rleValuesToConsume)
		{
			CompressedBufferOffsets.Add(compressedBufferOffset);
			DecompressedOffsets.Add(decompressedOffset);
			RleValuesToConsume.Add(rleValuesToConsume);
		}

		public void AnnotatePosition(long uncompressedOffset, long rleValuesToConsume)
		{
			CompressedBufferOffsets.Add(uncompressedOffset);
			RleValuesToConsume.Add(rleValuesToConsume);
		}

		public void FillPositionList(List<ulong> positions)
		{
			//If we weren't dealing with compressed data, only two values are written rather than three
			bool haveSecondValues = DecompressedOffsets.Count != 0;
			for (int i = 0; i < CompressedBufferOffsets.Count; i++)
			{
				positions.Add((ulong)CompressedBufferOffsets[i]);
				if(haveSecondValues)
					positions.Add((ulong)DecompressedOffsets[i]);
				positions.Add((ulong)RleValuesToConsume[i]);
			}
		}
	}
}
