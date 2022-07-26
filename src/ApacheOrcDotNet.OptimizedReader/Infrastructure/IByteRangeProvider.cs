using System;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public interface IByteRangeProvider : IDisposable
    {
        void FillBuffer(Span<byte> buffer, long position);
        Task FillBufferAsync(Memory<byte> buffer, long position);
        void FillBufferFromEnd(Span<byte> buffer);
        Task FillBufferFromEndAsync(Memory<byte> buffer);
    }
}
