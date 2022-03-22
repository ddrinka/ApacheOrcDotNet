using System;

namespace ApacheOrcDotNet.OptimizedReader
{
    public interface IByteRangeProvider : IDisposable
    {
        void GetRange(Span<byte> buffer, int position);
    }
}
