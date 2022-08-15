using System;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public interface IByteRangeProvider : IDisposable
    {
        void GetRange(Span<byte> buffer, long position);
        Task GetRangeAsync(Memory<byte> buffer, long position);
        void GetRangeFromEnd(Span<byte> buffer);
        Task GetRangeFromEndAsync(Memory<byte> buffer);
    }
}
