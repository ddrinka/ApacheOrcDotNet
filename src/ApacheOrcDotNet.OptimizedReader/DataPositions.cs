namespace ApacheOrcDotNet.OptimizedReader
{
    public readonly record struct DataPositions(int RowGroupOffset = 0, int RowEntryOffset = 0, int ValuesToSkip = 0, int RemainingBits = 0);
}
