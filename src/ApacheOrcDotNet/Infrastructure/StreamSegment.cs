using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Infrastructure
{
	public class StreamSegment : Stream
	{
		readonly Stream _underlyingStream;
		readonly long _lengthToExpose;
		readonly bool _keepUnderlyingStreamOpen;

		long _bytesRead = 0;

		public StreamSegment(Stream underlyingStream, long lengthToExpose, bool keepUnderlyingStreamOpen)
		{
			_underlyingStream = underlyingStream;
			_lengthToExpose = lengthToExpose;
			_keepUnderlyingStreamOpen = keepUnderlyingStreamOpen;

			if (!_underlyingStream.CanRead)
				throw new InvalidOperationException($"{nameof(StreamSegment)} requires a readable underlying stream");
		}

		public override bool CanRead => true;
		public override bool CanSeek => false;
		public override bool CanWrite => false;
		public override long Length => _lengthToExpose;		//What if the underlying Stream has less bytes available than this?
		public override long Position
		{
			get
			{
				return _bytesRead;
			}
			set
			{
				throw new NotImplementedException();
			}
		}
		public override int Read(byte[] buffer, int offset, int count)
		{
			if (_bytesRead >= _lengthToExpose)
				return 0;	//No more bytes

			var remainingBytes = _lengthToExpose - _bytesRead;
			var bytesToRead = (int)Math.Min((long)count, remainingBytes);   //Safe to cast to an int here because count can never exceed Int32.MaxValue
			var bytesRead = _underlyingStream.Read(buffer, offset, bytesToRead);
			_bytesRead += bytesRead;

			return bytesRead;
		}

		public override void Flush()
		{
			throw new NotImplementedException();
		}
		public override long Seek(long offset, SeekOrigin origin)
		{
			throw new NotImplementedException();
		}
		public override void SetLength(long value)
		{
			throw new NotImplementedException();
		}
		public override void Write(byte[] buffer, int offset, int count)
		{
			throw new NotImplementedException();
		}
		protected override void Dispose(bool disposing)
		{
			if (disposing && !_keepUnderlyingStreamOpen)
				_underlyingStream.Dispose();
			base.Dispose(disposing);
		}
	}
}
