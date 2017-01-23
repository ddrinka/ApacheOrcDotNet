using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Compression
{
	public class ZLib : ICompressor, IDecompressor
	{
		readonly CompressionLevel _compressionLevel;
		public ZLib(CompressionStrategy strategy)
		{
			switch (strategy)
			{
				case CompressionStrategy.Size: _compressionLevel = CompressionLevel.Optimal; break;
				case CompressionStrategy.Speed: _compressionLevel = CompressionLevel.Fastest; break;
				default: throw new NotImplementedException($"Unhandled {nameof(CompressionStrategy)} {strategy}");
			}
		}

		public bool Compress(byte[] inputBuffer, byte[] outputBuffer, byte[] overflow)
		{
			var output = new BufferStream(outputBuffer, overflow);
			using (var deflateStream = new DeflateStream(output, _compressionLevel))
			{
				deflateStream.Write(inputBuffer, 0, inputBuffer.Length);
			}
			return output.Length < inputBuffer.Length;
		}

		public void Decompress(byte[] inputBuffer, byte[] outputBuffer)
		{
			using (var inputStream = new MemoryStream(inputBuffer))
			using (var outputStream = new MemoryStream(outputBuffer))
			using (var inflateStream = new DeflateStream(inputStream, CompressionMode.Decompress))
			{
				inflateStream.CopyTo(outputStream);
			}
		}
	}

	internal class BufferStream : Stream
	{
		readonly byte[] _outputBuffer;
		readonly byte[] _overflowBuffer;
		int _outputBufferPosition = 0;
		int _overflowBufferPosition = 0;

		public BufferStream(byte[] outputBuffer, byte[] overflowBuffer)
		{
			_outputBuffer = outputBuffer;
			_overflowBuffer = overflowBuffer;
		}

		public override void Write(byte[] buffer, int offset, int count)
		{
			int outputSize = count;
			int overflowSize = 0;

			var outputBufferRemaining = _outputBuffer.Length - _outputBufferPosition;
			if (outputSize > outputBufferRemaining)
			{
				overflowSize = outputSize - outputBufferRemaining;
				outputSize = outputBufferRemaining;

				var overflowBufferRemaining = _overflowBuffer.Length - _overflowBufferPosition;
				if(overflowSize > overflowBufferRemaining)
					throw new OverflowException("Overflowed the overflow buffer");
			}

			if (outputSize != 0)
			{
				Buffer.BlockCopy(buffer, offset, _outputBuffer, _outputBufferPosition, outputSize);
				_outputBufferPosition += outputSize;
			}
			if (overflowSize != 0)
			{
				Buffer.BlockCopy(buffer, offset + outputSize, _overflowBuffer, _overflowBufferPosition, overflowSize);
				_overflowBufferPosition += overflowSize;
			}
		}

		public override long Length => _outputBufferPosition + _overflowBufferPosition;

		#region Unused Stream Overloads
		public override bool CanRead => false;
		public override bool CanSeek => false;
		public override bool CanWrite => true;
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
		}
		public override long Seek(long offset, SeekOrigin origin)
		{
			throw new NotImplementedException();
		}
		public override void SetLength(long value)
		{
			throw new NotImplementedException();
		}
		public override int Read(byte[] buffer, int offset, int count)
		{
			throw new NotImplementedException();
		}
		#endregion
	}
}
