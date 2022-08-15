using Xunit;

namespace ApacheOrcDotNet.OptimizedReader.Test.ColumnTypes.WithoutNulls
{
    public class IntegerColumn_Test : _BaseColumnTypeWithoutNulls
    {
        [Fact]
        public void Integer_Column_10k_Values()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("size");
            var columnBuffer = reader.CreateIntegerColumnBuffer(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 0, columnBuffer).Wait();

            Assert.Equal(10_000, columnBuffer.Values.Length);

            for (int i = 0; i < columnBuffer.Values.Length; i++)
            {
                Assert.NotNull(columnBuffer.Values[i]);
                Assert.Equal(long.Parse(ExpectedSizes[i], _invariantCulture), columnBuffer.Values[i]);
            }
        }

        [Fact]
        public void Integer_Column_1_Value()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("size");
            var columnBuffer = reader.CreateIntegerColumnBuffer(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 1, columnBuffer).Wait();

            Assert.Equal(1, columnBuffer.Values.Length);

            Assert.NotNull(columnBuffer.Values[0]);
            Assert.Equal(long.Parse(ExpectedSizes[10_000], _invariantCulture), columnBuffer.Values[0]);
        }
    }
}
