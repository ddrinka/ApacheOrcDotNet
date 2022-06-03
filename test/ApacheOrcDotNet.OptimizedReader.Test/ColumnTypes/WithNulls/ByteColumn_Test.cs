using Xunit;

namespace ApacheOrcDotNet.OptimizedReader.Test.ColumnTypes.WithNulls
{
    public class ByteColumn_Test : _BaseColumnTypeWithNulls
    {
        [Fact]
        public void Byte_Column_10k_Values()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("byte");
            var buffer = reader.CreateByteColumnBuffer(column);
            reader.FillBuffer(stripeId: 0, rowEntryIndexId: 0, buffer);

            Assert.Equal(10_000, buffer.Values.Length);

            for (int i = 0; i < buffer.Values.Length; i++)
            {
                if (_expectedValues.bytes[i] == null)
                    Assert.Null(buffer.Values[i]);
                else
                    Assert.Equal(byte.Parse(_expectedValues.bytes[i]), buffer.Values[i]);
            }
        }

        [Fact]
        public void Byte_Column_1_Value()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("byte");
            var buffer = reader.CreateByteColumnBuffer(column);
            reader.FillBuffer(stripeId: 0, rowEntryIndexId: 1, buffer);

            Assert.Equal(1, buffer.Values.Length);

            for (int i = 10_000; i < buffer.Values.Length; i++)
            {
                if (_expectedValues.bytes[i] == null)
                    Assert.Null(buffer.Values[i]);
                else
                    Assert.Equal(byte.Parse(_expectedValues.bytes[i]), buffer.Values[i]);
            }
        }
    }
}
