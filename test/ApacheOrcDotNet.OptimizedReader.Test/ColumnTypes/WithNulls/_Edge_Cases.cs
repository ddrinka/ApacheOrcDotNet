using Xunit;

namespace ApacheOrcDotNet.OptimizedReader.Test.ColumnTypes.WithNulls
{
    public class _Edge_Cases : _BaseColumnTypeWithNulls
    {
        [Fact]
        public void All_Values_Will_Track_Stride_Size()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var columnSize = reader.GetColumn("size");
            var columnSizeBuffer = reader.CreateIntegerColumnBuffer(columnSize);

            var columnBool = reader.GetColumn("boolean");
            var columnBoolBuffer = reader.CreateBooleanColumnReader(columnBool);

            // rowEntryIndexId '0' has '10_000' data rows.
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 0, columnBoolBuffer).Wait();
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 0, columnSizeBuffer).Wait();

            // The reader will track the entire stride.
            Assert.Equal(10_000, reader.NumValuesLoaded);
        }

        [Fact]
        public void Few_Values_Will_Track_Lower_Bound_Per_Stripe()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var columnSize = reader.GetColumn("size");
            var columnSizeBuffer = reader.CreateIntegerColumnBuffer(columnSize);

            var columnBool = reader.GetColumn("boolean");
            var columnBoolBuffer = reader.CreateBooleanColumnReader(columnBool);

            // rowEntryIndexId '1' has a single data row.
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 1, columnBoolBuffer).Wait();
            reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 1, columnSizeBuffer).Wait();

            // When present streams are available, all buffers will have at least 8 values loaded.
            Assert.Equal(8, reader.NumValuesLoaded);
        }
    }
}
