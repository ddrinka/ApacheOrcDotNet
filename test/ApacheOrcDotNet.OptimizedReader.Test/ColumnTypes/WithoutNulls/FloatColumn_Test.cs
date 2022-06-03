using Xunit;

namespace ApacheOrcDotNet.OptimizedReader.Test.ColumnTypes.WithoutNulls
{
    public class FloatColumn_Test : _BaseColumnTypeWithoutNulls
    {
        [Fact]
        public void Float_Column_10k_Values()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("float");
            var buffer = reader.CreateFloatColumnBuffer(column);
            reader.FillBuffer(stripeId: 0, rowEntryIndexId: 0, buffer);

            Assert.Equal(10_000, buffer.Values.Length);

            for (int i = 0; i < buffer.Values.Length; i++)
            {
                if (_expectedValues.floats[i] == null)
                    Assert.Equal(float.NaN, buffer.Values[i]);
                else
                    Assert.Equal(float.Parse(_expectedValues.floats[i], _enUSCulture), buffer.Values[i]);
            }
        }

        [Fact]
        public void Float_Column_1_Value()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("float");
            var buffer = reader.CreateFloatColumnBuffer(column);
            reader.FillBuffer(stripeId: 0, rowEntryIndexId: 1, buffer);

            Assert.Equal(1, buffer.Values.Length);

            for (int i = 10_000; i < buffer.Values.Length; i++)
            {
                if (_expectedValues.floats[i] == null)
                    Assert.Equal(float.NaN, buffer.Values[i]);
                else
                    Assert.Equal(float.Parse(_expectedValues.floats[i], _enUSCulture), buffer.Values[i]);
            }
        }
    }
}
