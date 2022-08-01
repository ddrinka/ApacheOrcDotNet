using Xunit;

namespace ApacheOrcDotNet.OptimizedReader.Test.ColumnTypes.WithNulls
{
    public class FloatColumn_Test : _BaseColumnTypeWithNulls
    {
        [Fact]
        public void Float_Column_10k_Values()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("float");
            var columnBuffer = reader.CreateFloatColumnBuffer(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 0, columnBuffer).Wait();

            Assert.Equal(10_000, columnBuffer.Values.Length);

            for (int i = 0; i < columnBuffer.Values.Length; i++)
            {
                if (ExpectedFloats[i] == null)
                    Assert.Null(columnBuffer.Values[i]);
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
            var columnBuffer = reader.CreateFloatColumnBuffer(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 1, columnBuffer).Wait();

            // When present streams are available, we will
            // always have at least 8 values in the
            // end of the values buffer.
            Assert.Equal(8, columnBuffer.Values.Length);

            // But we are only interested in the first here
            if (ExpectedFloats[10_000] == null)
                Assert.Null(columnBuffer.Values[0]);
            else
                Assert.Equal(float.Parse(ExpectedFloats[10_000], _invariantCulture), columnBuffer.Values[0]);
        }
    }
}
