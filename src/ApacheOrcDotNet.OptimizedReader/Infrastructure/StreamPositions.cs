namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public record StreamPositions(int RowGroupOffset = 0, int RowEntryOffset = 0, int ValuesToSkip = 0, int RemainingBits = 0);
}
