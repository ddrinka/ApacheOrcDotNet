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

        public int GetRange(Span<byte> buffer, long position)
        {
            lock (_streamLock)
            {
                _stream.Seek(position, SeekOrigin.Begin);
                return _stream.Read(buffer);
            }
        }

        public Task<int> GetRangeAsync(Memory<byte> buffer, long position)
        {
            lock (_streamLock)
            {
                _stream.Seek(position, SeekOrigin.Begin);
                return DoReadAsync(buffer);
            }
        }

        public int GetRangeFromEnd(Span<byte> buffer)
        {
            lock (_streamLock)
            {
                _stream.Seek(-buffer.Length, SeekOrigin.End);
                return _stream.Read(buffer);
            }
        }

        public Task<int> GetRangeFromEndAsync(Memory<byte> buffer)
        {
            lock (_streamLock)
            {
                _stream.Seek(-buffer.Length, SeekOrigin.End);
                return DoReadAsync(buffer);
            }
        }

        private async Task<int> DoReadAsync(Memory<byte> buffer)
        {
            int bytesRead = 0;
            int bytesRemaining = buffer.Length;
            while (bytesRemaining > 0)
            {
                int count = await _stream.ReadAsync(buffer[bytesRead..]);
                if (count == 0)
                    break;

                bytesRead += count;
                bytesRemaining -= count;
            }
            return bytesRead;
        }
    }
}
