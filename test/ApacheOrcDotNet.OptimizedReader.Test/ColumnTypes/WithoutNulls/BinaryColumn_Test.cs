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
            var buffer = reader.CreateBinaryColumnBuffer(column);
            reader.FillBuffer(stripeId: 0, rowEntryIndexId: 0, buffer);

            Assert.Equal(10_000, buffer.Values.Length);

            for (int i = 0; i < buffer.Values.Length; i++)
            {
                if (_expectedValues.binaries[i] == null)
                    Assert.Null(buffer.Values[i]);
                else
                    Assert.Equal(_expectedValues.binaries[i], Encoding.UTF8.GetString(buffer.Values[i]));
            }
        }

        [Fact]
        public void Binary_Column_1_Value()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("binary");
            var buffer = reader.CreateBinaryColumnBuffer(column);
            reader.FillBuffer(stripeId: 0, rowEntryIndexId: 1, buffer);

            Assert.Equal(1, buffer.Values.Length);

            for (int i = 10_000; i < buffer.Values.Length; i++)
            {
                if (_expectedValues.binaries[i] == null)
                    Assert.Null(buffer.Values[i]);
                else
                    Assert.Equal(_expectedValues.binaries[i], Encoding.UTF8.GetString(buffer.Values[i]));
            }
        }
    }
}
