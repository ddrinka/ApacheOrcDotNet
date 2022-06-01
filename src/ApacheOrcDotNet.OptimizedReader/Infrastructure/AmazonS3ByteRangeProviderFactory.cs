namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public sealed class AmazonS3ByteRangeProviderFactory : IByteRangeProviderFactory
    {
        public string Prefix => "";

        public IByteRangeProvider Create(string location)
        {
            return new AmazonS3ByteRangeProvider(location);
        }
    }
}
