using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Infrastructure
{
	/// <summary>
	/// A read-only Stream that calls out to a provider when data has been exhausted
	/// </summary>
    public class ConcatenatingStream : Stream
    {
		readonly Func<Stream> _nextStreamProvider;
		readonly bool _keepStreamsOpen;

		Stream _currentStream = null;
		bool _readingHasEnded = false;

		/// <summary>
		/// Create a Stream that passes data from a series of underlying Streams
		/// </summary>
		/// <param name="nextStreamProvider">A callback for the next underlying Stream.  Return null to indicate the Stream end.</param>
		/// <param name="keepStreamsOpen">Whether to leave underlying streams undisposed, or to dispose each after the last byte is read.</param>
		public ConcatenatingStream(Func<Stream> nextStreamProvider, bool keepStreamsOpen)
		{
			_nextStreamProvider = nextStreamProvider;
			_keepStreamsOpen = keepStreamsOpen;
		}

		public override int Read(byte[] buffer, int offset, int count)
		{
			if (_readingHasEnded)
				return 0;
			if (_currentStream == null)
			{
				_currentStream = _nextStreamProvider();
				if (_currentStream == null)
				{
					//No additional streams are available. Reading is done.
					_readingHasEnded = true;
					return 0;
				}
			}

			var bytesReadFromCurrentStream = _currentStream.Read(buffer, offset, count);
			if (bytesReadFromCurrentStream == 0)
			{
				if (!_keepStreamsOpen)
					_currentStream.Dispose();
				_currentStream = null;
				return Read(buffer, offset, count);     //Recurse, loading a new stream
			}
			else
				return bytesReadFromCurrentStream;
		}

		protected override void Dispose(bool disposing)
		{
			if (disposing)
			{
				_readingHasEnded = true;
				if (!_keepStreamsOpen)
					_currentStream.Dispose();
				_currentStream = null;
			}
			base.Dispose(disposing);
		}

		public override bool CanRead => true;
		public override bool CanSeek => false;
		public override bool CanWrite => false;
		public override long Length
		{
			get
			{
				throw new NotImplementedException();	//We can't determine the final length because we don't have access to future streams
			}
		}
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
	}
}
