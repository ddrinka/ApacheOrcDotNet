using System;

namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public sealed class ByteRangeProviderFactory : IByteRangeProviderFactory
    {
        public IByteRangeProvider Create(string location)
        {
            if (!Uri.TryCreate(location, UriKind.Absolute, out var uri))
                throw new InvalidOperationException($"Byte range provider must be a valid file:// or http(s):// URI.");

            var scheme = uri.Scheme;

            return scheme switch
            {
                "file" => new MemoryMappedFileRangeProvider(uri.LocalPath),
                "http" => new HttpByteRangeProvider(uri.AbsoluteUri),
                "https" => new HttpByteRangeProvider(uri.AbsoluteUri),
                _ => throw new InvalidOperationException($"The scheme '{scheme}' is not supported.")
            };
        }
    }
}
