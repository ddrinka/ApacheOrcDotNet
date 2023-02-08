using BenchmarkDotNet.Attributes;

namespace MicroBenchmarks
{
    [RankColumn]
    [MemoryDiagnoser]
    public class NullableBenchmarks
    {
        private const int size = 10_000;
        private readonly int[] _ints = new int[size];
        private readonly int?[] _nullInts = new int?[size];
        private readonly decimal[] _decimals = new decimal[size];
        private readonly decimal?[] _nullDecimals = new decimal?[size];
        private readonly double[] _doubles = new double[size];
        private readonly double?[] _nullDoubles = new double?[size];

        [Benchmark]
        public int IntAllocation()
        {
            for (int i = 0; i < size; i++)
                _ints[i] = Get(i, i);

            var temp = Get(_ints[3], _ints.Length);

            return temp;
        }

        [Benchmark]
        public int? IntAllocationNullable()
        {
            for (int i = 0; i < size; i++)
                _nullInts[i] = Get((int?)i, i);

            var temp = Get(_nullInts[3], _nullInts.Length);

            return temp;
        }

        [Benchmark]
        public decimal DecimalAllocation()
        {
            for (int i = 0; i < size; i++)
                _decimals[i] = Get(i, i);

            var temp = Get(_decimals[3], _decimals.Length);

            return temp;
        }

        [Benchmark]
        public decimal? DecimalAllocationNullable()
        {
            for (int i = 0; i < size; i++)
                _nullDecimals[i] = Get((decimal?)i, i);

            var temp = Get(_nullDecimals[3], _nullDecimals.Length);

            return temp;
        }

        [Benchmark]
        public double DoubleAllocation()
        {
            for (int i = 0; i < size; i++)
                _doubles[i] = Get((double)i, i);

            var temp = Get(_doubles[3], _doubles.Length);

            return temp;
        }

        [Benchmark]
        public double? DoubleAllocationNullable()
        {
            for (int i = 0; i < size; i++)
                _nullDoubles[i] = Get((double?)i, i);

            var temp = Get(_nullDoubles[3], _nullDoubles.Length);

            return temp;
        }

        private static T Get<T>(T input, int length)
        {
            // Attempt to prevent compiler optimization.
            if (length < 0)
                return default;

            return input;
        }
    }
}
