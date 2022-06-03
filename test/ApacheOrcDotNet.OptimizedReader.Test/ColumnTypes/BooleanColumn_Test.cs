using Xunit;

namespace ApacheOrcDotNet.OptimizedReader.Test.ColumnTypes
{
    public class BooleanColumn_Test : _BaseColumnType
    {
        [Fact]
        public void Boolean_Column_10k_Values()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("boolean");
            var buffer = reader.CreateBooleanColumnReader(column);
            reader.FillBuffer(stripeId: 0, rowEntryIndexId: 0, buffer);

            Assert.Equal(10_000, buffer.Values.Length);

            for (int i = 0; i < buffer.Values.Length; i++)
            {
                if (_expectedValues.booleans[i] == null)
                    Assert.Null(buffer.Values[i]);
                else
                    Assert.Equal(bool.Parse(_expectedValues.booleans[i]), buffer.Values[i]);
            }
        }

        [Fact]
        public void Boolean_Column_1_Value()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("boolean");
            var buffer = reader.CreateBooleanColumnReader(column);
            reader.FillBuffer(stripeId: 0, rowEntryIndexId: 1, buffer);

            Assert.Equal(1, buffer.Values.Length);

            for (int i = 10_000; i < buffer.Values.Length; i++)
            {
                if (_expectedValues.booleans[i] == null)
                    Assert.Null(buffer.Values[i]);
                else
                    Assert.Equal(bool.Parse(_expectedValues.booleans[i]), buffer.Values[i]);
            }
        }
    }
}
