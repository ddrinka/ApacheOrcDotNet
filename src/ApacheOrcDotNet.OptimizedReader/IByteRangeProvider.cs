using System;

namespace ApacheOrcDotNet.OptimizedReader
{
    public interface IByteRangeProvider : IDisposable
    {
        int GetRange(Span<byte> buffer, long position);
        int GetRangeFromEnd(Span<byte> buffer, long positionFromEnd);
    }
}
