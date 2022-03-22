namespace ApacheOrcDotNet.OptimizedReader
{
    public interface IByteRangeProviderFactory
    {
        string Prefix { get; }
        IByteRangeProvider Create(string location);
    }
}
