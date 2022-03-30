using ApacheOrcDotNet.Protocol;
using System;
using System.Linq;
using Xunit;

namespace ApacheOrcDotNet.OptimizedReader
{
    public class OrcReader_Test
    {
        readonly IByteRangeProvider _byteRangeProvider;
        public OrcReader_Test()
        {
            _byteRangeProvider = new TestByteRangeProvider(writeRequestedRangesToFile: false, readRequestedRangesFromFile: true);
        }

        [Fact]
        public void FileTail_DefaultOptomisticSize()
        {
            var reader = new OrcReader(new OrcReaderConfiguration(), _byteRangeProvider);
            var expectedColumns = new[]
            {
                new ColumnDetail(1, "Source", ColumnTypeKind.String),
                new ColumnDetail(2, "FeedId", ColumnTypeKind.Int),
                new ColumnDetail(3, "SequenceNumber", ColumnTypeKind.Long),
            };
            Assert.Equal(expectedColumns.ToList(), reader.ColumnDetails.Take(3).ToList());
        }

        [Fact]
        public void FileTail_MinimumInitialReadSize()
        {
            var reader = new OrcReader(new OrcReaderConfiguration { OptimisticFileTailReadLength = 1 }, _byteRangeProvider);
            var expectedColumns = new[]
            {
                new ColumnDetail(1, "Source", ColumnTypeKind.String),
                new ColumnDetail(2, "FeedId", ColumnTypeKind.Int),
                new ColumnDetail(3, "SequenceNumber", ColumnTypeKind.Long),
            };
            Assert.Equal(expectedColumns.ToList(), reader.ColumnDetails.Take(3).ToList());
        }

        [Fact]
        public void FileColumnStatistics()
        {
            var reader = new OrcReader(new OrcReaderConfiguration(), _byteRangeProvider);
            Assert.Equal("BZX", reader.GetFileColumnStatistics(1).StringStatistics.Minimum);
            Assert.Equal("UTDFNetworkC", reader.GetFileColumnStatistics(1).StringStatistics.Maximum);
            Assert.Equal(1, reader.GetFileColumnStatistics(2).IntStatistics.Minimum);
            Assert.Equal(35, reader.GetFileColumnStatistics(2).IntStatistics.Maximum);
            Assert.Equal(311, reader.GetFileColumnStatistics(3).IntStatistics.Minimum);
            Assert.Equal(596293502, reader.GetFileColumnStatistics(3).IntStatistics.Maximum);
            Assert.Equal(9.392m, decimal.Parse(reader.GetFileColumnStatistics(5).DecimalStatistics.Minimum));
            Assert.Equal(72041.725554m, decimal.Parse(reader.GetFileColumnStatistics(5).DecimalStatistics.Maximum));
        }

        [Fact]
        public void StripeColumnStatistics()
        {
            var reader = new OrcReader(new OrcReaderConfiguration(), _byteRangeProvider);
            Assert.Equal("BZX", reader.GetStripeColumnStatistics(1, 0).StringStatistics.Minimum);
            Assert.Equal("BZX", reader.GetStripeColumnStatistics(1, 0).StringStatistics.Maximum);
            Assert.Equal(1, reader.GetStripeColumnStatistics(2, 0).IntStatistics.Minimum);
            Assert.Equal(35, reader.GetStripeColumnStatistics(2, 0).IntStatistics.Maximum);
            Assert.Equal(311, reader.GetStripeColumnStatistics(3, 0).IntStatistics.Minimum);
            Assert.Equal(16690225, reader.GetStripeColumnStatistics(3, 0).IntStatistics.Maximum);
            Assert.Equal(25200.063318m, decimal.Parse(reader.GetStripeColumnStatistics(5, 0).DecimalStatistics.Minimum));
            Assert.Equal(71979.49409m, decimal.Parse(reader.GetStripeColumnStatistics(5, 0).DecimalStatistics.Maximum));
        }

        [Fact]
        public void RowGroupStatistics()
        {
            var reader = new OrcReader(new OrcReaderConfiguration(), _byteRangeProvider);
            var rowGroupDetails = reader.ReadRowGroupIndex(1, 0).ToList();
            var rowGroupDetail = rowGroupDetails[0];
            Assert.Equal("BZX", rowGroupDetail.Statistics.StringStatistics.Minimum);
            Assert.Equal("BZX", rowGroupDetail.Statistics.StringStatistics.Maximum);
            Assert.Equal(0, rowGroupDetail.StreamPositions[0].Position.DecompressedOffset);
            Assert.Equal(0, rowGroupDetail.StreamPositions[0].Position.ValueOffset);
        }
    }
}
