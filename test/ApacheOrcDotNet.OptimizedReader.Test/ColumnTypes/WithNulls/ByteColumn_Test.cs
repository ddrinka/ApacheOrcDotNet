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
            var columnBuffer = reader.CreateByteColumnBuffer(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 0, columnBuffer).Wait();

            Assert.Equal(10_000, columnBuffer.Values.Length);

            for (int i = 0; i < columnBuffer.Values.Length; i++)
            {
                if (ExpectedBytes[i] == null)
                    Assert.Null(columnBuffer.Values[i]);
                else
                    Assert.Equal(byte.Parse(ExpectedBytes[i]), columnBuffer.Values[i]);
            }
        }

        [Fact]
        public void Byte_Column_1_Value()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("byte");
            var columnBuffer = reader.CreateByteColumnBuffer(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 1, columnBuffer).Wait();

            // When present streams are available, we will
            // always have at least 8 values in the
            // end of the values buffer.
            Assert.Equal(8, columnBuffer.Values.Length);

            // But we are only interested in the first here
            if (ExpectedBytes[10_000] == null)
                Assert.Null(columnBuffer.Values[0]);
            else
                Assert.Equal(byte.Parse(ExpectedBytes[10_000]), columnBuffer.Values[0]);
        }
    }
}
