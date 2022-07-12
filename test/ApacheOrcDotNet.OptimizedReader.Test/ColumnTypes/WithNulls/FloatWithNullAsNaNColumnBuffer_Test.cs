using Xunit;

namespace ApacheOrcDotNet.OptimizedReader.Test.ColumnTypes.WithNulls
{
    public class FloatWithNullAsNaNColumnBuffer_Test : _BaseColumnTypeWithNulls
    {
        [Fact]
        public void Float_Column_10k_Values()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("float");
            var columnBuffer = reader.CreateFloatWithNullAsNaNColumnBuffer(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 0, columnBuffer).Wait();

            Assert.Equal(10_000, columnBuffer.Values.Length);

            for (int i = 0; i < columnBuffer.Values.Length; i++)
            {
                if (ExpectedFloats[i] == null)
                    Assert.Equal(float.NaN, columnBuffer.Values[i]);
                else
                    Assert.Equal(float.Parse(ExpectedFloats[i], _invariantCulture), columnBuffer.Values[i]);
            }
        }

        [Fact]
        public void Float_Column_1_Value()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("float");
            var columnBuffer = reader.CreateFloatWithNullAsNaNColumnBuffer(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 1, columnBuffer).Wait();

            Assert.Equal(1, columnBuffer.Values.Length);

            for (int i = 10_000; i < columnBuffer.Values.Length; i++)
            {
                if (ExpectedFloats[i] == null)
                    Assert.Equal(float.NaN, columnBuffer.Values[i]);
                else
                    Assert.Equal(float.Parse(ExpectedFloats[i], _invariantCulture), columnBuffer.Values[i]);
            }
        }
    }
}
