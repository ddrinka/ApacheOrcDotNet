using Xunit;

namespace ApacheOrcDotNet.OptimizedReader.Test.ColumnTypes.WithoutNulls
{
    public class DecimalAsDoubleColumn_Test : _BaseColumnTypeWithoutNulls
    {
        [Fact]
        public void DecimalAsDouble_Column_10k_Values()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("decimal");
            var columnBuffer = reader.CreateDecimalColumnBufferAsDouble(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 0, columnBuffer).Wait();

            Assert.Equal(10_000, columnBuffer.Values.Length);

            for (int i = 0; i < columnBuffer.Values.Length; i++)
            {
                Assert.NotEqual(double.NaN, columnBuffer.Values[i]);
                Assert.Equal(double.Parse(ExpectedTimesAsDouble[i], _invariantCulture), columnBuffer.Values[i]);
            }
        }

        [Fact]
        public void DecimalAsDouble_Column_1_Value()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("decimal");
            var columnBuffer = reader.CreateDecimalColumnBufferAsDouble(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 1, columnBuffer).Wait();

            Assert.Equal(1, columnBuffer.Values.Length);

            Assert.Equal(double.Parse(ExpectedTimesAsDouble[10_000], _invariantCulture), columnBuffer.Values[0]);
        }
    }
}
