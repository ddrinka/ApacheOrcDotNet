using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
	public class BinaryWriter : IColumnWriter<byte[]>
	{
		readonly bool _shouldAlignLengths;
		readonly OrcCompressedBuffer _presentBuffer;
		readonly OrcCompressedBuffer _dataBuffer;
		readonly OrcCompressedBuffer _lengthBuffer;

		public BinaryWriter(bool shouldAlignLengths, OrcCompressedBufferFactory bufferFactory, uint columnId)
		{
			_shouldAlignLengths = shouldAlignLengths;
			ColumnId = columnId;

			_presentBuffer = bufferFactory.CreateBuffer(StreamKind.Present);
			_presentBuffer.MustBeIncluded = false;
			_dataBuffer = bufferFactory.CreateBuffer(StreamKind.Data);
			_lengthBuffer = bufferFactory.CreateBuffer(StreamKind.Length);
		}

		public List<IStatistics> Statistics { get; } = new List<IStatistics>();
		public long CompressedLength => Buffers.Sum(s => s.Length);
		public uint ColumnId { get; }
		public OrcCompressedBuffer[] Buffers => new[] { _presentBuffer, _dataBuffer, _lengthBuffer };
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
			_presentBuffer.MustBeIncluded = false;
			Statistics.Clear();
		}

		public void AddBlock(IList<byte[]> values)
		{
			var stats = new BinaryWriterStatistics();
			Statistics.Add(stats);
            _presentBuffer.AnnotatePosition(stats, rleValuesToConsume: 0, bitsToConsume: 0);
            _dataBuffer.AnnotatePosition(stats);
            _lengthBuffer.AnnotatePosition(stats, rleValuesToConsume: 0);

			var bytesList = new List<byte[]>(values.Count);
			var presentList = new List<bool>(values.Count);
			var lengthList = new List<long>(values.Count);

			foreach (var bytes in values)
			{
				stats.AddValue(bytes);
				if (values != null)
				{
					bytesList.Add(bytes);
					lengthList.Add(bytes.Length);
				}
				presentList.Add(bytes != null);
			}

			var presentEncoder = new BitWriter(_presentBuffer);
			presentEncoder.Write(presentList);
			if (stats.HasNull)
				_presentBuffer.MustBeIncluded = true;

			foreach (var bytes in bytesList)
				_dataBuffer.Write(bytes, 0, bytes.Length);

			var lengthEncoder = new IntegerRunLengthEncodingV2Writer(_lengthBuffer);
			lengthEncoder.Write(lengthList, false, _shouldAlignLengths);
		}
	}
}