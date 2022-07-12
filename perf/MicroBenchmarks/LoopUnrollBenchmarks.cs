using BenchmarkDotNet.Attributes;
using System;

namespace MicroBenchmarks
{
    [RankColumn]
    [MemoryDiagnoser]
    public class LoopUnrollBenchmarks
    {
        [Benchmark]
        public void NormalLoop()
        {
            var numValuesRead = 0;
            var numByteValuesRead = 1000;
            Span<bool> outputValues = stackalloc bool[numByteValuesRead * 8];

            for (int idx = 0; idx < numByteValuesRead; idx++)
            {
                var decodedByte = 0xff;

                for (int bitIdx = 7; bitIdx >= 0; bitIdx--)
                {
                    outputValues[numValuesRead++] = (decodedByte & 1 << bitIdx) != 0;
                }
            }
        }

        [Benchmark]
        public void UnrolledLoop()
        {
            var numValuesRead = 0;
            var numByteValuesRead = 1000;
            Span<bool> outputValues = stackalloc bool[numByteValuesRead * 8];

            for (int idx = 0; idx < numByteValuesRead; idx++)
            {
                var decodedByte = 0xff;

                outputValues[numValuesRead++] = (decodedByte & 128) != 0;
                outputValues[numValuesRead++] = (decodedByte & 64) != 0;
                outputValues[numValuesRead++] = (decodedByte & 32) != 0;
                outputValues[numValuesRead++] = (decodedByte & 16) != 0;
                outputValues[numValuesRead++] = (decodedByte & 8) != 0;
                outputValues[numValuesRead++] = (decodedByte & 4) != 0;
                outputValues[numValuesRead++] = (decodedByte & 2) != 0;
                outputValues[numValuesRead++] = (decodedByte & 1) != 0;
            }
        }
    }
}
