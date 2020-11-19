namespace ApacheOrcDotNet.Statistics {
    public interface IDoubleStatistics {
        double? Minimum { get; }
        double? Maximum { get; }
        double? Sum { get; }
    }
}
