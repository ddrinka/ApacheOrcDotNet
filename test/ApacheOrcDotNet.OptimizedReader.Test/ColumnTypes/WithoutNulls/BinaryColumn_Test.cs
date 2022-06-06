using System.Text;
using Xunit;

namespace ApacheOrcDotNet.OptimizedReader.Test.ColumnTypes.WithoutNulls
{
    public class BinaryColumn_Test : _BaseColumnTypeWithoutNulls
    {
        [Fact]
        public void Binary_Column_10k_Values()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("binary");
            var columnBuffer = reader.CreateBinaryColumnBuffer(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 0, columnBuffer).Wait();
            reader.Parse(columnBuffer);

            Assert.Equal(10_000, columnBuffer.Values.Length);

            for (int i = 0; i < columnBuffer.Values.Length; i++)
            {
                if (_expectedValues.binaries[i] == null)
                    Assert.Null(columnBuffer.Values[i]);
                else
                    Assert.Equal(_expectedValues.binaries[i], Encoding.UTF8.GetString(columnBuffer.Values[i]));
            }
        }

        [Fact]
        public void Binary_Column_1_Value()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("binary");
            var columnBuffer = reader.CreateBinaryColumnBuffer(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 1, columnBuffer).Wait();
            reader.Parse(columnBuffer);

            Assert.Equal(1, columnBuffer.Values.Length);

            for (int i = 10_000; i < columnBuffer.Values.Length; i++)
            {
                if (_expectedValues.binaries[i] == null)
                    Assert.Null(columnBuffer.Values[i]);
                else
                    Assert.Equal(_expectedValues.binaries[i], Encoding.UTF8.GetString(columnBuffer.Values[i]));
            }
        }
    }
}
