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
            var buffer = reader.CreateDecimalColumnBufferAsDouble(column);
            reader.FillBuffer(stripeId: 0, rowEntryIndexId: 0, buffer);

            Assert.Equal(10_000, buffer.Values.Length);

            for (int i = 0; i < buffer.Values.Length; i++)
            {
                if (_expectedValues.timesAsDouble[i] == null)
                    Assert.Equal(double.NaN, buffer.Values[i]);
                else
                    Assert.Equal(double.Parse(_expectedValues.timesAsDouble[i], _enUSCulture), buffer.Values[i]);
            }
        }

        [Fact]
        public void DecimalAsDouble_Column_1_Value()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("time");
            var buffer = reader.CreateDecimalColumnBufferAsDouble(column);
            reader.FillBuffer(stripeId: 0, rowEntryIndexId: 1, buffer);

            Assert.Equal(1, buffer.Values.Length);

            for (int i = 10_000; i < buffer.Values.Length; i++)
            {
                if (_expectedValues.timesAsDouble[i] == null)
                    Assert.Equal(double.NaN, buffer.Values[i]);
                else
                    Assert.Equal(double.Parse(_expectedValues.timesAsDouble[i], _enUSCulture), buffer.Values[i]);
            }
        }
    }
}
