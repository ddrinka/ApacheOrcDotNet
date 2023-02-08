using Xunit;

namespace ApacheOrcDotNet.OptimizedReader.Test.ColumnTypes.WithNulls
{
    public class StringColumn_Test : _BaseColumnTypeWithNulls
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
                if (ExpectedSymbols[i] == null)
                    Assert.Null(columnBuffer.Values[i]);
                else
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

            // When present streams are available, we will
            // always have at least 8 values in the
            // end of the values buffer.
            Assert.Equal(8, columnBuffer.Values.Length);

            // But we are only interested in the first here
            if (ExpectedSymbols[10_000] == null)
                Assert.Null(columnBuffer.Values[0]);
            else
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
                if (ExpectedSources[i] == null)
                    Assert.Null(columnBuffer.Values[i]);
                else
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

            // When present streams are available, we will
            // always have at least 8 values in the
            // end of the values buffer.
            Assert.Equal(8, columnBuffer.Values.Length);

            // But we are only interested in the first here
            if (ExpectedSources[10_000] == null)
                Assert.Null(columnBuffer.Values[0]);
            else
                Assert.Equal(ExpectedSources[10_000], columnBuffer.Values[0]);
        }
    }
}
