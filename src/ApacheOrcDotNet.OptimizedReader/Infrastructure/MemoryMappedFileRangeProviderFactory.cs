namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public sealed class MemoryMappedFileRangeProviderFactory : IByteRangeProviderFactory
    {
        public string Prefix => "file://";

        public IByteRangeProvider Create(string location)
        {
            return new MemoryMappedFileRangeProvider(location[Prefix.Length..]);
        }
    }
}
