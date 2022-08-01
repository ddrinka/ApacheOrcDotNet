using Xunit;

namespace ApacheOrcDotNet.OptimizedReader.Test.ColumnTypes.WithoutNulls
{
    public class BooleanColumn_Test : _BaseColumnTypeWithoutNulls
    {
        [Fact]
        public void Boolean_Column_10k_Values()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("boolean");
            var columnBuffer = reader.CreateBooleanColumnReader(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 0, columnBuffer).Wait();

            Assert.Equal(10_000, columnBuffer.Values.Length);

            for (int i = 0; i < columnBuffer.Values.Length; i++)
            {
                Assert.NotNull(columnBuffer.Values[i]);
                Assert.Equal(bool.Parse(ExpectedBooleans[i]), columnBuffer.Values[i]);
            }
        }

        [Fact]
        public void Boolean_Column_1_Value()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("boolean");
            var columnBuffer = reader.CreateBooleanColumnReader(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 1, columnBuffer).Wait();

            // We will always have at least 8 bits being processed.
            Assert.Equal(8, columnBuffer.Values.Length);

            Assert.NotNull(columnBuffer.Values[0]);
            Assert.Equal(bool.Parse(ExpectedBooleans[10_000]), columnBuffer.Values[0]);
        }
    }
}
