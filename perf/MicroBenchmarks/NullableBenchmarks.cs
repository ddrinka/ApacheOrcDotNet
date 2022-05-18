using BenchmarkDotNet.Attributes;

namespace MicroBenchmarks
{
    [RankColumn]
    [MemoryDiagnoser]
    public class NullableBenchmarks
    {
        [Benchmark]
        public int IntAllocation()
        {
            return int.MaxValue;
        }

        [Benchmark]
        public int? IntAllocationNullable()
        {
            int? value = null;
            return value;
        }

        [Benchmark]
        public decimal DecimalAllocation()
        {
            return decimal.MaxValue;
        }

        [Benchmark]
        public decimal? DecimalAllocationNullable()
        {
            decimal? value = null;
            return value;
        }

        [Benchmark]
        public double DoubleAllocation()
        {
            return double.MaxValue;
        }

        [Benchmark]
        public double? DoubleAllocationNullable()
        {
            double? value = null;
            return value;
        }
    }
}
