using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class BinaryWriter : ColumnWriter<byte[]>
    {
		readonly bool _shouldAlignLengths;
		readonly OrcCompressedBuffer _presentBuffer;
		readonly OrcCompressedBuffer _dataBuffer;
		readonly OrcCompressedBuffer _lengthBuffer;

		public BinaryWriter(bool shouldAlignLengths, OrcCompressedBufferFactory bufferFactory, uint columnId)
			: base(bufferFactory, columnId)
		{
			_shouldAlignLengths = shouldAlignLengths;

			_presentBuffer = bufferFactory.CreateBuffer(StreamKind.Present);
			_presentBuffer.MustBeIncluded = false;
			_dataBuffer = bufferFactory.CreateBuffer(StreamKind.Data);
			_lengthBuffer = bufferFactory.CreateBuffer(StreamKind.Length);
		}

		protected override ColumnEncodingKind DetectEncodingKind(IList<byte[]> values)
		{
			return ColumnEncodingKind.DirectV2;
		}

		protected override void AddDataStreamBuffers(IList<OrcCompressedBuffer> buffers, ColumnEncodingKind encodingKind)
		{
			if (encodingKind != ColumnEncodingKind.DirectV2)
				throw new NotSupportedException($"Only DirectV2 encoding is supported for {nameof(BinaryWriter)}");

			buffers.Add(_presentBuffer);
			buffers.Add(_dataBuffer);
			buffers.Add(_lengthBuffer);
		}

		protected override IStatistics CreateStatistics() => new BinaryWriterStatistics();

		protected override void EncodeValues(IList<byte[]> values, IList<OrcCompressedBuffer> buffers, IStatistics statistics)
		{
			var stats = (BinaryWriterStatistics)statistics;

			var bytesList = new List<byte[]>(values.Count);
			var presentList = new List<bool>(values.Count);
			var lengthList = new List<long>(values.Count);

			foreach(var bytes in values)
			{
				stats.AddValue(bytes);
				if(values!=null)
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
