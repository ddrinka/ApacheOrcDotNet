namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public interface IByteRangeProviderFactory
    {
        string Prefix { get; }
        IByteRangeProvider Create(string location);
    }
}
