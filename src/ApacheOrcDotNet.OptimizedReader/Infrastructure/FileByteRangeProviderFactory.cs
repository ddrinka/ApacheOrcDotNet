namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public sealed class FileByteRangeProviderFactory : IByteRangeProviderFactory
    {
        public string Prefix => "file://";

        public IByteRangeProvider Create(string location)
        {
            return new FileByteRangeProvider(location[Prefix.Length..]);
        }
    }
}
