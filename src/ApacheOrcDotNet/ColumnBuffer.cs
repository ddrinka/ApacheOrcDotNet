using ApacheOrcDotNet.Compression;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet
{
    public class ColumnBuffer<T>
    {
		readonly int _indexStride;
		readonly int _compressionBlockSize;
		readonly int _stripeStreamSize;
		readonly IStatisticsFactory<T> _statisticsFactory;
		readonly IValueEncoder<T> _valueEncoder;
		readonly Protocol.CompressionKind _compressionKind;
		readonly CompressionStrategy _compressionStrategy;

		readonly List<T> _valueBlock = new List<T>();
		IStatistics<T> _statistics;

		public ColumnBuffer(int indexStride, int compressionBlockSize, int stripeStreamSize, IStatisticsFactory<T> statisticsFactory, IValueEncoder<T> valueEncoder, Protocol.CompressionKind compressionKind, CompressionStrategy compressionStrategy)
		{
			_indexStride = indexStride;
			_compressionBlockSize = compressionBlockSize;
			_stripeStreamSize = stripeStreamSize;
			_statisticsFactory = statisticsFactory;
			_valueEncoder = valueEncoder;
			_compressionKind = compressionKind;
			_compressionStrategy = compressionStrategy;

			_statistics = statisticsFactory.CreateStatistics();
		}

		public bool AddValue(T value)
		{
			_valueBlock.Add(value);
			_statistics.AddValue(value);

			if (_valueBlock.Count > _indexStride)
			{
				var areDoneWithStripe = ProcessBlock();
				if (areDoneWithStripe)
					return false;
			}

			return true;
		}

		bool ProcessBlock()
		{
			throw new NotImplementedException();
		}
    }
}
