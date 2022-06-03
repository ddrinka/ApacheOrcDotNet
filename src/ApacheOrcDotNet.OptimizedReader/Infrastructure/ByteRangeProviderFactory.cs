using System;

namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public sealed class ByteRangeProviderFactory : IByteRangeProviderFactory
    {
        public IByteRangeProvider Create(string location)
        {
            if (!Uri.TryCreate(location, UriKind.Absolute, out var uri))
                throw new InvalidOperationException($"Byte range provider must be a valid file:// or http(s):// URI.");

            if (uri.IsFile)
                return new MemoryMappedFileRangeProvider(uri.LocalPath);

            return new HttpByteRangeProvider(uri.AbsoluteUri);
        }
    }
}
