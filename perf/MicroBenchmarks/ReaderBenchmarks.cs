using BenchmarkDotNet.Attributes;

namespace MicroBenchmarks
{
    [RankColumn]
    [MemoryDiagnoser]
    public class ReaderBenchmarks
    {
        [Benchmark]
        public int Standard() => default; // TODO: Add Benchmarks.
    }
}
