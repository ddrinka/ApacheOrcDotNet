﻿using Xunit;

namespace ApacheOrcDotNet.OptimizedReader.Test.ColumnTypes
{
    public class StringColumn_Test : _BaseColumnType
    {
        [Fact]
        public void String_DirectV2_Column_10k_Values()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("symbol");
            var buffer = reader.CreateStringColumnBuffer(column);
            reader.FillBuffer(stripeId: 0, rowEntryIndexId: 0, buffer);

            Assert.Equal(10_000, buffer.Values.Length);

            for (int i = 0; i < buffer.Values.Length; i++)
            {
                if (_expectedValues.symbols[i] == null)
                    Assert.Null(buffer.Values[i]);
                else
                    Assert.Equal(_expectedValues.symbols[i], buffer.Values[i]);
            }
        }

        [Fact]
        public void String_DirectV2_Column_1_Value()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("symbol");
            var buffer = reader.CreateStringColumnBuffer(column);
            reader.FillBuffer(stripeId: 0, rowEntryIndexId: 1, buffer);

            Assert.Equal(1, buffer.Values.Length);

            for (int i = 10_000; i < buffer.Values.Length; i++)
            {
                if (_expectedValues.symbols[i] == null)
                    Assert.Null(buffer.Values[i]);
                else
                    Assert.Equal(_expectedValues.symbols[i], buffer.Values[i]);
            }
        }

        [Fact]
        public void String_DictionaryV2_Column_10k_Values()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("source");
            var buffer = reader.CreateStringColumnBuffer(column);
            reader.FillBuffer(stripeId: 0, rowEntryIndexId: 0, buffer);

            Assert.Equal(10_000, buffer.Values.Length);

            for (int i = 0; i < buffer.Values.Length; i++)
            {
                if (_expectedValues.sources[i] == null)
                    Assert.Null(buffer.Values[i]);
                else
                    Assert.Equal(_expectedValues.sources[i], buffer.Values[i]);
            }
        }

        [Fact]
        public void String_DictionaryV2_Column_1_Value()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("source");
            var buffer = reader.CreateStringColumnBuffer(column);
            reader.FillBuffer(stripeId: 0, rowEntryIndexId: 1, buffer);

            Assert.Equal(1, buffer.Values.Length);

            for (int i = 10_000; i < buffer.Values.Length; i++)
            {
                if (_expectedValues.sources[i] == null)
                    Assert.Null(buffer.Values[i]);
                else
                    Assert.Equal(_expectedValues.sources[i], buffer.Values[i]);
            }
        }
    }
}
