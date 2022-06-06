using System;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public interface IByteRangeProvider : IDisposable
    {
        int GetRange(Span<byte> buffer, long position);
        Task<int> GetRangeAsync(Memory<byte> buffer, long position);
        int GetRangeFromEnd(Span<byte> buffer, long positionFromEnd);
        Task<int> GetRangeFromEndAsync(Memory<byte> buffer, long positionFromEnd);
    }
}
