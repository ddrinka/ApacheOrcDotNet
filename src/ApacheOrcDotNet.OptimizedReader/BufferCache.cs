using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System;

namespace ApacheOrcDotNet.OptimizedReader
{
    public class BufferCache
    {
        public StreamRange[] _ranges;

        public BufferCache()
        {
            var streamKinds = Enum.GetValues<StreamKind>().Length;
            _ranges = new StreamRange[streamKinds];
        }

        public bool IsMatch(StreamDetail stream)
            => _ranges[(int)stream.StreamKind] == stream.Range;

        public void Set(StreamDetail stream)
            => _ranges[(int)stream.StreamKind] = stream.Range;
    }
}
