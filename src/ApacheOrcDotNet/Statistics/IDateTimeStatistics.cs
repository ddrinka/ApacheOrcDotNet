using System;

namespace ApacheOrcDotNet.Statistics {
    public interface IDateTimeStatistics
    {
		DateTime? Minimum { get; }
		DateTime? Maximum { get; }
    }
}
