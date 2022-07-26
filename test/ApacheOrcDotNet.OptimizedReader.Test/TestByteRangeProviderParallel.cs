using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Test.TestHelpers;
using System;
using System.IO;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader
{
    public sealed class TestByteRangeProviderParallel : IByteRangeProvider
    {
        readonly object _streamLock = new object();
        readonly Stream _stream;

        internal TestByteRangeProviderParallel(string fileName)
        {
            var dataFileHelper = new DataFileHelper(typeof(TestByteRangeProvider), fileName);
            _stream = dataFileHelper.GetStream();
        }

        public void Dispose()
        {
            _stream.Dispose();
        }

        public void FillBuffer(Span<byte> buffer, long position)
        {
            lock (_streamLock)
            {
                _stream.Seek(position, SeekOrigin.Begin);
                var bytesRead = _stream.Read(buffer);
                if (bytesRead < buffer.Length)
                    throw new InvalidOperationException("Insufficient data to fill the buffer.");
            }
        }

        public Task FillBufferAsync(Memory<byte> buffer, long position)
        {
            lock (_streamLock)
            {
                _stream.Seek(position, SeekOrigin.Begin);
                return DoReadAsync(buffer);
            }
        }

        public void FillBufferFromEnd(Span<byte> buffer)
        {
            lock (_streamLock)
            {
                _stream.Seek(-buffer.Length, SeekOrigin.End);
                var bytesRead = _stream.Read(buffer);
                if (bytesRead < buffer.Length)
                    throw new BufferNotFilledException();
            }
        }

        public Task FillBufferFromEndAsync(Memory<byte> buffer)
        {
            lock (_streamLock)
            {
                _stream.Seek(-buffer.Length, SeekOrigin.End);
                return DoReadAsync(buffer);
            }
        }

        private async Task DoReadAsync(Memory<byte> buffer)
        {
            var bytesRead = await _stream.ReadAsync(buffer);
            if (bytesRead < buffer.Length)
                throw new BufferNotFilledException();
        }
    }
}
