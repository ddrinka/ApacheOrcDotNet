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
            _length = (new FileInfo(path)).Length;
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
                return stream.Read(buffer);
            }
        }

        public async Task<int> GetRangeAsync(Memory<byte> buffer, long position)
        {
            using (var stream = _memoryMappedFile.CreateViewStream(position, buffer.Length, MemoryMappedFileAccess.Read))
            {
                return await stream.ReadAsync(buffer);
            }
        }

        public int GetRangeFromEnd(Span<byte> buffer)
        {
            using (var stream = _memoryMappedFile.CreateViewStream(_length - buffer.Length, buffer.Length, MemoryMappedFileAccess.Read))
            {
                return stream.Read(buffer);
            }
        }

        public async Task<int> GetRangeFromEndAsync(Memory<byte> buffer)
        {
            using (var stream = _memoryMappedFile.CreateViewStream(_length - buffer.Length, buffer.Length, MemoryMappedFileAccess.Read))
            {
                return await stream.ReadAsync(buffer);
            }
        }
    }
}
