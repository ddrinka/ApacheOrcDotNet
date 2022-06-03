namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public interface IByteRangeProviderFactory
    {
        IByteRangeProvider Create(string location);
    }
}
