using ApacheOrcDotNet.Compression;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Stripes
{
    public class StripeStreamWriter<T>
    {
		readonly int _indexRowStride;
		readonly int _compressionBlockSize;
		readonly IStatisticsFactory<T> _statisticsFactory;
		readonly IValueEncoder<T> _valueEncoder;
		readonly Protocol.CompressionKind _compressionKind;
		readonly CompressionStrategy _compressionStrategy;
		readonly MemoryStream _encodedBlock = new MemoryStream();

		bool _blockAddingIsComplete = false;

		public StripeStreamWriter(int indexRowStride, int compressionBlockSize, IStatisticsFactory<T> statisticsFactory, IValueEncoder<T> valueEncoder, Protocol.CompressionKind compressionKind, CompressionStrategy compressionStrategy)
		{
			_indexRowStride = indexRowStride;
			_compressionBlockSize = compressionBlockSize;
			_statisticsFactory = statisticsFactory;
			_valueEncoder = valueEncoder;
			_compressionKind = compressionKind;
			_compressionStrategy = compressionStrategy;
		}

		List<IStatistics<T>> Statistics { get; } = new List<IStatistics<T>>();
		public MemoryStream StripeStreamBuffer { get; } = new MemoryStream();

		/// <summary>
		/// Write a full index stride's worth of data.  This should be called repeatedly until the StripeStreamBuffer's length is satisfactory.
		/// </summary>
		/// <param name="values">A block of values to process and compress</param>
		public void AddBlock(ArraySegment<T> values)
		{
			if (_blockAddingIsComplete)
				throw new InvalidOperationException("Attempted to add blocks after calling CompleteAddingBlocks");

			var statistics = _statisticsFactory.CreateStatistics();
			statistics.ProcessValues(values);
			EnsureEncodedBlockLengthIsBelowBlockSize();
			_valueEncoder.EncodeValues(values, _encodedBlock);
			statistics.AnnotatePosition(StripeStreamBuffer.Length, _encodedBlock.Length, 0);    //Our implementation always ends the RLE at the stride
			Statistics.Add(statistics);
		}

		/// <summary>
		/// Inform the writer that no further blocks will be delivered--Compress all buffered data and conclude processing.
		/// </summary>
		public void CompleteAddingBlocks()
		{
			_blockAddingIsComplete = true;
			CompressCurrentBlockAndReset();
		}

		void EnsureEncodedBlockLengthIsBelowBlockSize()
		{
			if (_encodedBlock.Length < _compressionBlockSize)
				return;

			CompressCurrentBlockAndReset();
		}

		void CompressCurrentBlockAndReset()
		{
			//Compress the encoded block and write it to the StreamBuffer
			OrcCompressedStream.CompressCopyTo(_encodedBlock, StripeStreamBuffer, _compressionKind, _compressionStrategy);
			_encodedBlock.SetLength(0);
		}
	}
}
