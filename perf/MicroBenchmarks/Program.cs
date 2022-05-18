using BenchmarkDotNet.Running;

namespace MicroBenchmarks
{
    internal class Program
    {
        static void Main(string[] args)
        {
            _ = BenchmarkRunner.Run<NullableBenchmarks>();
        }
    }
}
