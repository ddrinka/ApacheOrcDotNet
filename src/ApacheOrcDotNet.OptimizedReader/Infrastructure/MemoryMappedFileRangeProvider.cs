using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public sealed class MemoryMappedFileRangeProvider : IByteRangeProvider
    {
        readonly MemoryMappedFile _memoryMappedFile;
        readonly long _length;

        internal MemoryMappedFileRangeProvider(string path)
        {
            using (var fileStream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read))
                _length = fileStream.Length;

            _memoryMappedFile = MemoryMappedFile.CreateFromFile(path);
        }

        public void Dispose()
        {
            _memoryMappedFile.Dispose();
        }

        public int GetRange(Span<byte> buffer, long position)
        {
            using (var stream = _memoryMappedFile.CreateViewStream(position, buffer.Length, MemoryMappedFileAccess.Read))
            {
                return DoRead(stream, buffer);
            }
        }

        public async Task<int> GetRangeAsync(Memory<byte> buffer, long position)
        {
            using (var stream = _memoryMappedFile.CreateViewStream(position, buffer.Length, MemoryMappedFileAccess.Read))
            {
                return await DoReadAsync(stream, buffer);
            }
        }

        public int GetRangeFromEnd(Span<byte> buffer, long positionFromEnd)
        {
            using (var stream = _memoryMappedFile.CreateViewStream(_length - positionFromEnd, buffer.Length, MemoryMappedFileAccess.Read))
            {
                return DoRead(stream, buffer);
            }
        }

        public async Task<int> GetRangeFromEndAsync(Memory<byte> buffer, long positionFromEnd)
        {
            using (var stream = _memoryMappedFile.CreateViewStream(_length - positionFromEnd, buffer.Length, MemoryMappedFileAccess.Read))
            {
                return await DoReadAsync(stream, buffer);
            }
        }

        private int DoRead(Stream stream, Span<byte> buffer)
        {
            int bytesRead = 0;
            int bytesRemaining = buffer.Length;
            while (bytesRemaining > 0)
            {
                int count = stream.Read(buffer[bytesRead..]);
                if (count == 0)
                    break;

                bytesRead += count;
                bytesRemaining -= count;
            }
            return bytesRead;
        }

        private async Task<int> DoReadAsync(Stream stream, Memory<byte> buffer)
        {
            int bytesRead = 0;
            int bytesRemaining = buffer.Length;
            while (bytesRemaining > 0)
            {
                int count = await stream.ReadAsync(buffer[bytesRead..]);
                if (count == 0)
                    break;

                bytesRead += count;
                bytesRemaining -= count;
            }
            return bytesRead;
        }
    }
}
