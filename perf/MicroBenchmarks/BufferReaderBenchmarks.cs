using ApacheOrcDotNet.OptimizedReader;
using BenchmarkDotNet.Attributes;
using System.IO;

namespace MicroBenchmarks
{
    [RankColumn]
    [MemoryDiagnoser]
    public class BufferReaderBenchmarks
    {
        private readonly byte[] _readBuffer1 = new byte[3];
        private readonly byte[] _readBuffer2 = new byte[3];
        private readonly string _testFilePath = @"BenchmarkDotNet.SampleData/optimized_reader_test_file.orc";
        private readonly byte[] _testBuffer;

        public BufferReaderBenchmarks()
        {
            using (var fileStream = File.OpenRead(_testFilePath))
            {
                _testBuffer = new byte[fileStream.Length];
                fileStream.Read(_testBuffer);
            }
        }

        [Benchmark]
        public void TryRead()
        {
            var reader = new BufferReader(_testBuffer);
            while (reader.TryRead(out _)) { }
        }

        [Benchmark]
        public void TryCopyTo()
        {
            var reader = new BufferReader(_testBuffer);
            while (reader.TryReadTo(_readBuffer1)) { }
        }

        [Benchmark]
        public void TryRead3Bytes()
        {
            var reader = new BufferReader(_testBuffer);

            _ = reader.TryRead(out _);
            _ = reader.TryRead(out _);
            _ = reader.TryRead(out _);
        }

        [Benchmark]
        public void TryCopyTo3Bytes()
        {
            var reader = new BufferReader(_testBuffer);
            _ = reader.TryReadTo(_readBuffer2);
        }
    }
}
