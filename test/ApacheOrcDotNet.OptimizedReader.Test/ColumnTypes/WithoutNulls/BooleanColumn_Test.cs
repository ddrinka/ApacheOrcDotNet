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
            reader.Fill(columnBuffer);

            Assert.Equal(10_000, columnBuffer.Values.Length);

            for (int i = 0; i < columnBuffer.Values.Length; i++)
            {
                if (_expectedValues.booleans[i] == null)
                    Assert.Null(columnBuffer.Values[i]);
                else
                    Assert.Equal(bool.Parse(_expectedValues.booleans[i]), columnBuffer.Values[i]);
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
            reader.Fill(columnBuffer);

            Assert.Equal(1, columnBuffer.Values.Length);

            for (int i = 10_000; i < columnBuffer.Values.Length; i++)
            {
                if (_expectedValues.booleans[i] == null)
                    Assert.Null(columnBuffer.Values[i]);
                else
                    Assert.Equal(bool.Parse(_expectedValues.booleans[i]), columnBuffer.Values[i]);
            }
        }
    }
}
