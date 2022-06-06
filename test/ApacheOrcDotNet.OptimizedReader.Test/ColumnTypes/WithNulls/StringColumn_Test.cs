﻿using Xunit;

namespace ApacheOrcDotNet.OptimizedReader.Test.ColumnTypes.WithNulls
{
    public class StringColumn_Test : _BaseColumnTypeWithNulls
    {
        [Fact]
        public void String_DirectV2_Column_10k_Values()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("symbol");
            var columnBuffer = reader.CreateStringColumnBuffer(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 0, columnBuffer).Wait();
            reader.Parse(columnBuffer);

            Assert.Equal(10_000, columnBuffer.Values.Length);

            for (int i = 0; i < columnBuffer.Values.Length; i++)
            {
                if (_expectedValues.symbols[i] == null)
                    Assert.Null(columnBuffer.Values[i]);
                else
                    Assert.Equal(_expectedValues.symbols[i], columnBuffer.Values[i]);
            }
        }

        [Fact]
        public void String_DirectV2_Column_1_Value()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("symbol");
            var columnBuffer = reader.CreateStringColumnBuffer(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 1, columnBuffer).Wait();
            reader.Parse(columnBuffer);

            Assert.Equal(1, columnBuffer.Values.Length);

            for (int i = 10_000; i < columnBuffer.Values.Length; i++)
            {
                if (_expectedValues.symbols[i] == null)
                    Assert.Null(columnBuffer.Values[i]);
                else
                    Assert.Equal(_expectedValues.symbols[i], columnBuffer.Values[i]);
            }
        }

        [Fact]
        public void String_DictionaryV2_Column_10k_Values()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("source");
            var columnBuffer = reader.CreateStringColumnBuffer(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 0, columnBuffer).Wait();
            reader.Parse(columnBuffer);

            Assert.Equal(10_000, columnBuffer.Values.Length);

            for (int i = 0; i < columnBuffer.Values.Length; i++)
            {
                if (_expectedValues.sources[i] == null)
                    Assert.Null(columnBuffer.Values[i]);
                else
                    Assert.Equal(_expectedValues.sources[i], columnBuffer.Values[i]);
            }
        }

        [Fact]
        public void String_DictionaryV2_Column_1_Value()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("source");
            var columnBuffer = reader.CreateStringColumnBuffer(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 1, columnBuffer).Wait();
            reader.Parse(columnBuffer);

            Assert.Equal(1, columnBuffer.Values.Length);

            for (int i = 10_000; i < columnBuffer.Values.Length; i++)
            {
                if (_expectedValues.sources[i] == null)
                    Assert.Null(columnBuffer.Values[i]);
                else
                    Assert.Equal(_expectedValues.sources[i], columnBuffer.Values[i]);
            }
        }
    }
}
