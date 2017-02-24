using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Compression
{
	public class ZLibStream : Stream
	{
		readonly bool _areCompressing;
		readonly Stream _deflateStream;

		public ZLibStream(CompressionStrategy strategy, Stream outputStream)
		{
			_areCompressing = true;
			CompressionLevel compressionLevel;
			switch (strategy)
			{
				case CompressionStrategy.Size: compressionLevel = CompressionLevel.Optimal; break;
				case CompressionStrategy.Speed: compressionLevel = CompressionLevel.Fastest; break;
				default: throw new NotImplementedException($"Unhandled {nameof(CompressionStrategy)} {strategy}");
			}
			_deflateStream = new DeflateStream(outputStream, compressionLevel, true);
		}

		public ZLibStream(Stream inputStream)
		{
			_areCompressing = false;
			_deflateStream = new DeflateStream(inputStream, CompressionMode.Decompress, true);
		}

		public override bool CanRead => _areCompressing ? false : true;
		public override bool CanSeek => false;
		public override bool CanWrite => _areCompressing ? true : false;
		public override long Length => _deflateStream.Length;
		public override long Position
		{
			get
			{
				throw new NotImplementedException();
			}
			set
			{
				throw new NotImplementedException();
			}
		}
		public override void Flush() => _deflateStream.Flush();
		public override long Seek(long offset, SeekOrigin origin) => _deflateStream.Seek(offset, origin);
		public override void SetLength(long value) => _deflateStream.SetLength(value);
		public override int Read(byte[] buffer, int offset, int count)
		{
			if (_areCompressing)
				throw new InvalidOperationException("Cannot read from a compressing stream");
			return _deflateStream.Read(buffer, offset, count);
		}
		public override void Write(byte[] buffer, int offset, int count)
		{
			if (!_areCompressing)
				throw new InvalidOperationException("Cannot write to a decompressing stream");
			_deflateStream.Write(buffer, offset, count);
		}
		protected override void Dispose(bool disposing)
		{
			if (disposing)
				_deflateStream.Dispose();

			base.Dispose(disposing);
		}
	}
}
