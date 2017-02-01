using ApacheOrcDotNet.Compression;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Compression
{
    public class OrcCompressedBuffer : Stream
    {
		readonly int _compressionBlockSize;
		readonly Protocol.CompressionKind _compressionKind;
		readonly CompressionStrategy _compressionStrategy;
		readonly MemoryStream _currentBlock = new MemoryStream();

		bool _doneWriting = false;

		public OrcCompressedBuffer(int compressionBlockSize, Protocol.CompressionKind compressionKind, CompressionStrategy compressionStrategy)
		{
			_compressionBlockSize = compressionBlockSize;
			_compressionKind = compressionKind;
			_compressionStrategy = compressionStrategy;
		}

		public MemoryStream CompressedBuffer { get; } = new MemoryStream();
		public long CurrentBlockLength => _currentBlock.Length;

		public override void Write(byte[] buffer, int offset, int count)
		{
			if (_doneWriting)
				throw new InvalidOperationException("Tried to write after WritingCompleted()");

			var spaceRemaining = _compressionBlockSize - (int)_currentBlock.Length;
			if (spaceRemaining < 0)
				throw new ArithmeticException("A code error has led to negative space remaining");

			if(count >= spaceRemaining)
			{
				_currentBlock.Write(buffer, offset, spaceRemaining);
				CompressCurrentBlockAndReset();
				count -= spaceRemaining;
				offset += spaceRemaining;
			}
			if (count > 0)
				_currentBlock.Write(buffer, offset, count);
		}

		public void WritingCompleted()
		{
			CompressCurrentBlockAndReset();
			_doneWriting = true;
		}

		void CompressCurrentBlockAndReset()
		{
			//Compress the encoded block and write it to the CompressedBuffer
			OrcCompressedStream.CompressCopyTo(_currentBlock, CompressedBuffer, _compressionKind, _compressionStrategy);
			_currentBlock.SetLength(0);
		}

		#region Stream Implementation
		public override bool CanRead => false;
		public override bool CanSeek => false;
		public override bool CanWrite => true;
		public override long Length
		{
			get
			{
				throw new NotImplementedException();
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
		public override int Read(byte[] buffer, int offset, int count)
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
		#endregion
	}
}
