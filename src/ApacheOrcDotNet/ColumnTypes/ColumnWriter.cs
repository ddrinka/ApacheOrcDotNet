using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Statistics;
using System;
using System.Collections.Generic;
using System.Linq;

namespace ApacheOrcDotNet.ColumnTypes
{
	public abstract class ColumnWriter<T>
	{
		readonly OrcCompressedBuffer[] _stripeStreamBuffers;

		bool _blockAddingIsComplete = false;

		public ColumnWriter()
		{
			_stripeStreamBuffers = new OrcCompressedBuffer[NumDataStreams];
		}

		public List<IStatistics> Statistics { get; } = new List<IStatistics>();
		public long CompressedLength => _stripeStreamBuffers.Sum(b => b.CompressedBuffer.Length);

		protected abstract IStatistics CreateStatistics();
		protected abstract void EncodeValues(IList<T> values, OrcCompressedBuffer[] buffers, IStatistics statistics);
		protected abstract int NumDataStreams { get; }

		/// <summary>
		/// Write a full index stride's worth of data.  This should be called repeatedly until the sum of all buffers in all columns is greater than the stripe size
		/// </summary>
		/// <param name="values">A block of values to process</param>
		public void AddBlock(IList<T> values)
		{
			if (_blockAddingIsComplete)
				throw new InvalidOperationException("Attempted to add blocks after calling CompleteAddingBlocks");

			var statistics = CreateStatistics();
			foreach (var buffer in _stripeStreamBuffers)
				statistics.AnnotatePosition(buffer.CompressedBuffer.Length, buffer.CurrentBlockLength, 0);    //Our implementation always ends the RLE at the stride

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
	}

	public class ColumnWriterStatistics
	{
		public long CompressedBufferOffset { get; private set; }
		public long DecompressedOffset { get; private set; }
		public long RleValuesToConsume { get; private set; }

		public void AnnotatePosition(long compressedBufferOffset, long decompressedOffset, long rleValuesToConsume)
		{
			CompressedBufferOffset = compressedBufferOffset;
			DecompressedOffset = decompressedOffset;
			RleValuesToConsume = rleValuesToConsume;
		}
	}
}
