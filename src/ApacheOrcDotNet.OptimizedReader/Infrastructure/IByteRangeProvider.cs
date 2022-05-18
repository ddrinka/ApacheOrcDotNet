using System;

namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public interface IByteRangeProvider : IDisposable
    {
        int GetRange(Span<byte> buffer, long position);
        int GetRangeFromEnd(Span<byte> buffer, long positionFromEnd);
    }
}
