using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using BenchmarkDotNet.Attributes;
using System;
using System.Collections.Generic;

/*
|                              Method |      Mean |     Error |    StdDev | Rank | Allocated |
|------------------------------------ |----------:|----------:|----------:|-----:|----------:|
|                       ArrayEquality |  7.720 ns | 0.0755 ns | 0.0706 ns |    1 |         - |
|    DictionaryContainsKeyAndEquality | 12.429 ns | 0.0690 ns | 0.0645 ns |    2 |         - |
|               DictionaryTryGetValue | 13.052 ns | 0.0677 ns | 0.0633 ns |    3 |         - |
 */

namespace MicroBenchmarks
{
    [RankColumn]
    [MemoryDiagnoser]
    public class ComparisonBenchmarks
    {
        private const int size = 255;
        private readonly StreamRange[] _buffer = new StreamRange[size];
        private readonly Dictionary<int, StreamRange> _buffer2 = new();
        private readonly StreamRange _testRange = new StreamRange(255, 255, 255);

        public ComparisonBenchmarks()
        {
            for (int i = 0; i < size; i++)
            {
                _buffer[i] = new StreamRange(i, i, i);
                _buffer2.Add(i, new StreamRange(i, i, i));
            }
        }

        [Benchmark]
        public bool ArrayEquality() => _buffer[128] == _testRange;

        [Benchmark]
        public bool DictionaryContainsKeyAndEquality() => _buffer2.ContainsKey(128) && _buffer[128] == _testRange;

        [Benchmark]
        public bool DictionaryTryGetValue() => _buffer2.TryGetValue(128, out var range) && range == _testRange;
    }
}
