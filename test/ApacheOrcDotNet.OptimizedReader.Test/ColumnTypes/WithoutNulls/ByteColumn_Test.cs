﻿using Xunit;

namespace ApacheOrcDotNet.OptimizedReader.Test.ColumnTypes.WithoutNulls
{
    public class ByteColumn_Test : _BaseColumnTypeWithoutNulls
    {
        [Fact]
        public void Byte_Column_10k_Values()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("byte");
            var columnBuffer = reader.CreateByteColumnBuffer(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 0, columnBuffer).Wait();

            Assert.Equal(10_000, columnBuffer.Values.Length);

            for (int i = 0; i < columnBuffer.Values.Length; i++)
            {
                Assert.NotNull(columnBuffer.Values[i]);
                Assert.Equal(byte.Parse(ExpectedBytes[i]), columnBuffer.Values[i]);
            }
        }

        [Fact]
        public void Byte_Column_1_Value()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("byte");
            var columnBuffer = reader.CreateByteColumnBuffer(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 1, columnBuffer).Wait();

            Assert.Equal(1, columnBuffer.Values.Length);

            Assert.NotNull(columnBuffer.Values[0]);
            Assert.Equal(byte.Parse(ExpectedBytes[10_000]), columnBuffer.Values[0]);
        }
    }
}
