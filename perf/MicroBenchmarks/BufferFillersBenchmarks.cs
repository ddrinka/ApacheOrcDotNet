using BenchmarkDotNet.Attributes;
using System;

/*
|         Method |        Mean |     Error |    StdDev | Rank | Allocated |
|--------------- |------------:|----------:|----------:|-----:|----------:|
|   FillWithLoop | 29,797.8 ns | 481.98 ns | 427.27 ns |    2 |         - |
| FillWithMethod |    902.8 ns |  17.79 ns |  28.73 ns |    1 |         - |
 */

namespace MicroBenchmarks
{
    [RankColumn]
    [MemoryDiagnoser]
    public class BufferFillersBenchmarks
    {
        private const int size = 65_536;
        private readonly byte[] _buffer = new byte[size];

        [Benchmark]
        public void FillWithLoop()
        {
            for (int i = 0; i < size; i++)
                _buffer[i] = 255;
        }

        [Benchmark]
        public void FillWithMethod()
        {
            _buffer.AsSpan().Fill(255);
        }
    }
}
