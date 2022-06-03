using Xunit;

namespace ApacheOrcDotNet.OptimizedReader.Test.ColumnTypes
{
    public class DecimalColumn_Test : _BaseColumnType
    {
        [Fact]
        public void Decimal_Column_10k_Values()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("time");
            var buffer = reader.CreateDecimalColumnReader(column);
            reader.FillBuffer(stripeId: 0, rowEntryIndexId: 0, buffer);

            Assert.Equal(10_000, buffer.Values.Length);

            for (int i = 0; i < buffer.Values.Length; i++)
            {
                if (_expectedValues.times[i] == null)
                    Assert.Null(buffer.Values[i]);
                else
                    Assert.Equal(decimal.Parse(_expectedValues.times[i], _enUSCulture), buffer.Values[i]);
            }
        }

        [Fact]
        public void Decimal_Column_1_Value()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("time");
            var buffer = reader.CreateDecimalColumnReader(column);
            reader.FillBuffer(stripeId: 0, rowEntryIndexId: 1, buffer);

            Assert.Equal(1, buffer.Values.Length);

            for (int i = 10_000; i < buffer.Values.Length; i++)
            {
                if (_expectedValues.times[i] == null)
                    Assert.Null(buffer.Values[i]);
                else
                    Assert.Equal(decimal.Parse(_expectedValues.times[i], _enUSCulture), buffer.Values[i]);
            }
        }
    }
}
