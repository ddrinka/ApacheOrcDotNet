using Xunit;

namespace ApacheOrcDotNet.OptimizedReader.Test.ColumnTypes.WithoutNulls
{
    public class StringColumn_Test : _BaseColumnTypeWithoutNulls
    {
        [Fact]
        public void String_DirectV2_Column_10k_Values()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("stringDirectV2");
            var columnBuffer = reader.CreateStringColumnBuffer(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 0, columnBuffer).Wait();

            Assert.Equal(10_000, columnBuffer.Values.Length);

            for (int i = 0; i < columnBuffer.Values.Length; i++)
            {
                Assert.NotNull(columnBuffer.Values[i]);
                Assert.Equal(ExpectedSymbols[i], columnBuffer.Values[i]);
            }
        }

        [Fact]
        public void String_DirectV2_Column_1_Value()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("stringDirectV2");
            var columnBuffer = reader.CreateStringColumnBuffer(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 1, columnBuffer).Wait();

            Assert.Equal(1, columnBuffer.Values.Length);

            Assert.NotNull(columnBuffer.Values[0]);
            Assert.Equal(ExpectedSymbols[10_000], columnBuffer.Values[0]);
        }

        [Fact]
        public void String_DictionaryV2_Column_10k_Values()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("stringDictionaryV2");
            var columnBuffer = reader.CreateStringColumnBuffer(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 0, columnBuffer).Wait();

            Assert.Equal(10_000, columnBuffer.Values.Length);

            for (int i = 0; i < columnBuffer.Values.Length; i++)
            {
                Assert.NotNull(columnBuffer.Values[i]);
                Assert.Equal(ExpectedSources[i], columnBuffer.Values[i]);
            }
        }

        [Fact]
        public void String_DictionaryV2_Column_1_Value()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("stringDictionaryV2");
            var columnBuffer = reader.CreateStringColumnBuffer(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 1, columnBuffer).Wait();

            Assert.Equal(1, columnBuffer.Values.Length);

            Assert.NotNull(columnBuffer.Values[0]);
            Assert.Equal(ExpectedSources[10_000], columnBuffer.Values[0]);
        }
    }
}
