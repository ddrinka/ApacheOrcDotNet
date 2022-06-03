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
            var buffer = reader.CreateIntegerColumnBuffer(column);
            reader.FillBuffer(stripeId: 0, rowEntryIndexId: 0, buffer);

            Assert.Equal(10_000, buffer.Values.Length);

            for (int i = 0; i < buffer.Values.Length; i++)
            {
                if (_expectedValues.sizes[i] == null)
                    Assert.Null(buffer.Values[i]);
                else
                    Assert.Equal(long.Parse(_expectedValues.sizes[i], _enUSCulture), buffer.Values[i]);
            }
        }

        [Fact]
        public void Integer_Column_1_Value()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("size");
            var buffer = reader.CreateIntegerColumnBuffer(column);
            reader.FillBuffer(stripeId: 0, rowEntryIndexId: 1, buffer);

            Assert.Equal(1, buffer.Values.Length);

            for (int i = 10_000; i < buffer.Values.Length; i++)
            {
                if (_expectedValues.sizes[i] == null)
                    Assert.Null(buffer.Values[i]);
                else
                    Assert.Equal(long.Parse(_expectedValues.sizes[i], _enUSCulture), buffer.Values[i]);
            }
        }
    }
}
