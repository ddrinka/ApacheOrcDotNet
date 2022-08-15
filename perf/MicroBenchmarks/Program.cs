using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;
using System.Diagnostics;

namespace MicroBenchmarks
{
    internal class Program
    {
        static void Main(string[] args)
        {
            IConfig config = Debugger.IsAttached
                ? new DebugInProcessConfig()
                : DefaultConfig.Instance;

            BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
        }
    }
}
