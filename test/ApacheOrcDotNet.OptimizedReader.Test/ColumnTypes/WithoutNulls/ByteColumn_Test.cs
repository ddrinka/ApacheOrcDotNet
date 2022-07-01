using Xunit;

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
            reader.Fill(columnBuffer);

            Assert.Equal(10_000, columnBuffer.Values.Length);

            for (int i = 0; i < columnBuffer.Values.Length; i++)
            {
                if (ExpectedBytes[i] == null)
                    Assert.Null(columnBuffer.Values[i]);
                else
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
            reader.Fill(columnBuffer);

            Assert.Equal(1, columnBuffer.Values.Length);

            for (int i = 10_000; i < columnBuffer.Values.Length; i++)
            {
                if (ExpectedBytes[i] == null)
                    Assert.Null(columnBuffer.Values[i]);
                else
                    Assert.Equal(byte.Parse(ExpectedBytes[i]), columnBuffer.Values[i]);
            }
        }
    }
}
