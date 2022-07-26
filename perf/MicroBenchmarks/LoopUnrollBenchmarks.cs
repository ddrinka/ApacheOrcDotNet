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
            var decodedByte = 0xff;
            var numValuesRead = 0;
            var numByteValuesRead = 1000;
            Span<bool> outputValues = stackalloc bool[numByteValuesRead * 8];

            for (int idx = 0; idx < numByteValuesRead; idx++)
            {
                outputValues = outputValues.Slice(8);
                if (outputValues.Length < 8)
                    break;

                outputValues[0] = (decodedByte & 128) != 0;
                outputValues[1] = (decodedByte & 64) != 0;
                outputValues[2] = (decodedByte & 32) != 0;
                outputValues[3] = (decodedByte & 16) != 0;
                outputValues[4] = (decodedByte & 8) != 0;
                outputValues[5] = (decodedByte & 4) != 0;
                outputValues[6] = (decodedByte & 2) != 0;
                outputValues[7] = (decodedByte & 1) != 0;

                numValuesRead += 8;
            }
        }
    }
}
