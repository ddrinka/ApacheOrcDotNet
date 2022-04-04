using ApacheOrcDotNet.OptimizedReader.Encodings;
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

        [Fact]
        public void IntegerRunLengthEncoding_Read_ShortRepeat()
        {
            Span<byte> input = stackalloc byte[] { 0x0a, 0x27, 0x10 };
            Span<long> expected = stackalloc long[] { 10000, 10000, 10000, 10000, 10000 };
            Span<long> output = stackalloc long[5];

            var position = new Position(ChunkFileOffset: 0, DecompressedOffset: null, ValueOffset: 0, ValueOffset2: null);
            SpanIntegerRunLengthEncodingV2.ReadValues(input, position, isSigned: false, output);

            for (int i = 0; i < expected.Length; i++)
                Assert.Equal(expected[i], output[i]);
        }

        [Fact]
        public void IntegerRunLengthEncoding_Read_Direct()
        {
            Span<byte> input = stackalloc byte[] { 0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef };
            Span<long> expected = stackalloc long[] { 23713, 43806, 57005, 48879 };
            Span<long> output = stackalloc long[4];

            var position = new Position(ChunkFileOffset: 0, DecompressedOffset: null, ValueOffset: 0, ValueOffset2: null);
            SpanIntegerRunLengthEncodingV2.ReadValues(input, position, isSigned: false, output);

            for (int i = 0; i < expected.Length; i++)
                Assert.Equal(expected[i], output[i]);
        }

        [Fact]
        public void IntegerRunLengthEncoding_Read_PatchedBase()
        {
            Span<byte> input = stackalloc byte[] { 0x8e, 0x13, 0x2b, 0x21, 0x07, 0xd0, 0x1e, 0x00, 0x14, 0x70, 0x28, 0x32, 0x3c, 0x46, 0x50, 0x5a, 0x64, 0x6e, 0x78, 0x82, 0x8c, 0x96, 0xa0, 0xaa, 0xb4, 0xbe, 0xfc, 0xe8 };
            Span<long> expected = stackalloc long[] { 2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090, 2100, 2110, 2120, 2130, 2140, 2150, 2160, 2170, 2180, 2190 };
            Span<long> output = stackalloc long[20];

            var position = new Position(ChunkFileOffset: 0, DecompressedOffset: null, ValueOffset: 0, ValueOffset2: null);
            SpanIntegerRunLengthEncodingV2.ReadValues(input, position, isSigned: false, output);

            for (int i = 0; i < expected.Length; i++)
                Assert.Equal(expected[i], output[i]);
        }

        [Fact]
        public void IntegerRunLengthEncoding_Read_Delta()
        {
            Span<byte> input = stackalloc byte[] { 0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46 };
            Span<long> expected = stackalloc long[] { 2, 3, 5, 7, 11, 13, 17, 19, 23, 29 };
            Span<long> output = stackalloc long[10];

            var position = new Position(ChunkFileOffset: 0, DecompressedOffset: null, ValueOffset: 0, ValueOffset2: null);
            SpanIntegerRunLengthEncodingV2.ReadValues(input, position, isSigned: false, output);

            for (int i = 0; i < expected.Length; i++)
                Assert.Equal(expected[i], output[i]);
        }
    }
}
