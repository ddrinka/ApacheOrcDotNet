using Xunit;

namespace ApacheOrcDotNet.OptimizedReader.Test.ColumnTypes.WithNulls
{
    public class DecimalAsDoubleColumn_Test : _BaseColumnTypeWithNulls
    {
        [Fact]
        public void DecimalAsDouble_Column_10k_Values()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("time");
            var columnBuffer = reader.CreateDecimalColumnBufferAsDouble(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 0, columnBuffer).Wait();
            reader.Fill(columnBuffer);

            Assert.Equal(10_000, columnBuffer.Values.Length);

            for (int i = 0; i < columnBuffer.Values.Length; i++)
            {
                if (ExpectedTimesAsDouble[i] == null)
                    Assert.Equal(double.NaN, columnBuffer.Values[i]);
                else
                    Assert.Equal(double.Parse(ExpectedTimesAsDouble[i], _enUSCulture), columnBuffer.Values[i]);
            }
        }

        [Fact]
        public void DecimalAsDouble_Column_1_Value()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("time");
            var columnBuffer = reader.CreateDecimalColumnBufferAsDouble(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 1, columnBuffer).Wait();
            reader.Fill(columnBuffer);

            Assert.Equal(1, columnBuffer.Values.Length);

            for (int i = 10_000; i < columnBuffer.Values.Length; i++)
            {
                if (ExpectedTimesAsDouble[i] == null)
                    Assert.Equal(double.NaN, columnBuffer.Values[i]);
                else
                    Assert.Equal(double.Parse(ExpectedTimesAsDouble[i], _enUSCulture), columnBuffer.Values[i]);
            }
        }
    }
}
