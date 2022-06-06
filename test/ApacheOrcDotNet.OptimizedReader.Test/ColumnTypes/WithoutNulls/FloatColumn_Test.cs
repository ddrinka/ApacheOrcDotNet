﻿using Xunit;

namespace ApacheOrcDotNet.OptimizedReader.Test.ColumnTypes.WithoutNulls
{
    public class FloatColumn_Test : _BaseColumnTypeWithoutNulls
    {
        [Fact]
        public void Float_Column_10k_Values()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("float");
            var columnBuffer = reader.CreateFloatColumnBuffer(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 0, columnBuffer).Wait();
            reader.Fill(columnBuffer);

            Assert.Equal(10_000, columnBuffer.Values.Length);

            for (int i = 0; i < columnBuffer.Values.Length; i++)
            {
                if (_expectedValues.floats[i] == null)
                    Assert.Equal(float.NaN, columnBuffer.Values[i]);
                else
                    Assert.Equal(float.Parse(_expectedValues.floats[i], _enUSCulture), columnBuffer.Values[i]);
            }
        }

        [Fact]
        public void Float_Column_1_Value()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("float");
            var columnBuffer = reader.CreateFloatColumnBuffer(column);
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 1, columnBuffer).Wait();
            reader.Fill(columnBuffer);

            Assert.Equal(1, columnBuffer.Values.Length);

            for (int i = 10_000; i < columnBuffer.Values.Length; i++)
            {
                if (_expectedValues.floats[i] == null)
                    Assert.Equal(float.NaN, columnBuffer.Values[i]);
                else
                    Assert.Equal(float.Parse(_expectedValues.floats[i], _enUSCulture), columnBuffer.Values[i]);
            }
        }
    }
}
