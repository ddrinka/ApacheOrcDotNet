using Xunit;

namespace ApacheOrcDotNet.OptimizedReader.Test.ColumnTypes.WithoutNulls
{
    public class DecimalColumn_Test : _BaseColumnTypeWithoutNulls
    {
        [Fact]
        public void Decimal_Column_10k_Values()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("time");
            var columnBuffer = reader.CreateDecimalColumnBuffer(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 0, columnBuffer).Wait();

            Assert.Equal(10_000, columnBuffer.Values.Length);

            for (int i = 0; i < columnBuffer.Values.Length; i++)
            {
                Assert.NotNull(columnBuffer.Values[i]);
                Assert.Equal(decimal.Parse(ExpectedTimes[i], _invariantCulture), columnBuffer.Values[i]);
            }
        }

        [Fact]
        public void Decimal_Column_1_Value()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("time");
            var columnBuffer = reader.CreateDecimalColumnBuffer(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 1, columnBuffer).Wait();

            Assert.Equal(1, columnBuffer.Values.Length);

            Assert.NotNull(columnBuffer.Values[0]);
            Assert.Equal(decimal.Parse(ExpectedTimes[10_000], _invariantCulture), columnBuffer.Values[0]);
        }
    }
}
