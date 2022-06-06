using Xunit;

namespace ApacheOrcDotNet.OptimizedReader.Test.ColumnTypes.WithoutNulls
{
    public class DoubleColumn_Test : _BaseColumnTypeWithoutNulls
    {
        [Fact]
        public void Double_Column_10k_Values()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("double");
            var columnBuffer = reader.CreateDoubleColumnBuffer(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 0, columnBuffer).Wait();
            reader.Parse(columnBuffer);

            Assert.Equal(10_000, columnBuffer.Values.Length);

            for (int i = 0; i < columnBuffer.Values.Length; i++)
            {
                if (_expectedValues.doubles[i] == null)
                    Assert.Equal(double.NaN, columnBuffer.Values[i]);
                else
                    Assert.Equal(double.Parse(_expectedValues.doubles[i], _enUSCulture), columnBuffer.Values[i]);
            }
        }

        [Fact]
        public void Double_Column_1_Value()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("double");
            var columnBuffer = reader.CreateDoubleColumnBuffer(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 1, columnBuffer).Wait();
            reader.Parse(columnBuffer);

            Assert.Equal(1, columnBuffer.Values.Length);

            for (int i = 10_000; i < columnBuffer.Values.Length; i++)
            {
                if (_expectedValues.doubles[i] == null)
                    Assert.Equal(double.NaN, columnBuffer.Values[i]);
                else
                    Assert.Equal(double.Parse(_expectedValues.doubles[i], _enUSCulture), columnBuffer.Values[i]);
            }
        }
    }
}
