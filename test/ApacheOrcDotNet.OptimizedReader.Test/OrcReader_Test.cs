using ApacheOrcDotNet.OptimizedReader.Encodings;
using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System;
using System.Globalization;
using System.Linq;
using Xunit;

namespace ApacheOrcDotNet.OptimizedReader
{
    public class OrcReader_Test
    {
        private readonly CultureInfo _enUSCulture = CultureInfo.GetCultureInfo("en-US");
        private readonly IByteRangeProvider _byteRangeProvider = new TestByteRangeProvider(writeRequestedRangesToFile: false, readRequestedRangesFromFile: true);

        [Fact]
        public void FileTail_DefaultOptomisticSize()
        {
            var reader = new OrcReader(new OrcOptimizedReaderConfiguration(), _byteRangeProvider);
            var expectedColumns = new[]
            {
                new OrcColumn(1, "Source", ColumnTypeKind.String),
                new OrcColumn(2, "FeedId", ColumnTypeKind.Int),
                new OrcColumn(3, "SequenceNumber", ColumnTypeKind.Long)
            };
            var existingColumns = new[]
            {
                reader.GetColumn("Source"),
                reader.GetColumn("FeedId"),
                reader.GetColumn("SequenceNumber")
            };
            Assert.Equal<OrcColumn>(expectedColumns.ToList(), existingColumns.ToList());
        }

        [Fact]
        public void FileTail_MinimumInitialReadSize()
        {
            var reader = new OrcReader(new OrcOptimizedReaderConfiguration { OptimisticFileTailReadLength = 1 }, _byteRangeProvider);
            var expectedColumns = new[]
            {
                new OrcColumn(1, "Source", ColumnTypeKind.String),
                new OrcColumn(2, "FeedId", ColumnTypeKind.Int),
                new OrcColumn(3, "SequenceNumber", ColumnTypeKind.Long)
            };
            var existingColumns = new[]
            {
                reader.GetColumn("Source"),
                reader.GetColumn("FeedId"),
                reader.GetColumn("SequenceNumber")
            };
            Assert.Equal(expectedColumns.ToList(), existingColumns.ToList());
        }

        [Fact]
        public void FileColumnStatistics()
        {
            var reader = new OrcReader(new OrcOptimizedReaderConfiguration(), _byteRangeProvider);
            Assert.Equal("BZX", reader.GetFileColumnStatistics(1).StringStatistics.Minimum);
            Assert.Equal("UTDFNetworkC", reader.GetFileColumnStatistics(1).StringStatistics.Maximum);
            Assert.Equal(1, reader.GetFileColumnStatistics(2).IntStatistics.Minimum);
            Assert.Equal(35, reader.GetFileColumnStatistics(2).IntStatistics.Maximum);
            Assert.Equal(311, reader.GetFileColumnStatistics(3).IntStatistics.Minimum);
            Assert.Equal(596293502, reader.GetFileColumnStatistics(3).IntStatistics.Maximum);
            Assert.Equal(9.392m, decimal.Parse(reader.GetFileColumnStatistics(5).DecimalStatistics.Minimum, _enUSCulture));
            Assert.Equal(72041.725554m, decimal.Parse(reader.GetFileColumnStatistics(5).DecimalStatistics.Maximum, _enUSCulture));
        }

        [Fact]
        public void StripeColumnStatistics()
        {
            var reader = new OrcReader(new OrcOptimizedReaderConfiguration(), _byteRangeProvider);
            Assert.Equal("BZX", reader.GetStripeColumnStatistics(1, 0).StringStatistics.Minimum);
            Assert.Equal("BZX", reader.GetStripeColumnStatistics(1, 0).StringStatistics.Maximum);
            Assert.Equal(1, reader.GetStripeColumnStatistics(2, 0).IntStatistics.Minimum);
            Assert.Equal(35, reader.GetStripeColumnStatistics(2, 0).IntStatistics.Maximum);
            Assert.Equal(311, reader.GetStripeColumnStatistics(3, 0).IntStatistics.Minimum);
            Assert.Equal(16690225, reader.GetStripeColumnStatistics(3, 0).IntStatistics.Maximum);
            Assert.Equal(25200.063318m, decimal.Parse(reader.GetStripeColumnStatistics(5, 0).DecimalStatistics.Minimum, _enUSCulture));
            Assert.Equal(71979.49409m, decimal.Parse(reader.GetStripeColumnStatistics(5, 0).DecimalStatistics.Maximum, _enUSCulture));
        }

        [Fact]
        public void RowGroupStatistics()
        {
            var reader = new OrcReader(new OrcOptimizedReaderConfiguration(), _byteRangeProvider);
            var rowGroupEntry = reader.GetRowGroupIndex(columnId: 1, stripeId: 0).Entry[0];
            Assert.Equal("BZX", rowGroupEntry.Statistics.StringStatistics.Minimum);
            Assert.Equal("BZX", rowGroupEntry.Statistics.StringStatistics.Maximum);
            Assert.Equal(0ul, rowGroupEntry.Positions[0]);
            Assert.Equal(0ul, rowGroupEntry.Positions[1]);
        }

        [Fact]
        public void IntegerRLE_Read_ShortRepeat()
        {
            Span<byte> input = stackalloc byte[] { 0x0a, 0x27, 0x10 };
            Span<long> expected = stackalloc long[] { 10000, 10000, 10000, 10000, 10000 };
            Span<long> output = stackalloc long[5];

            var bufferReader = new BufferReader(input);
            var numValuesRead = OptimizedIntegerRLE.ReadValues(ref bufferReader, isSigned: false, output);

            Assert.Equal(expected.Length, numValuesRead);
            for (int i = 0; i < numValuesRead; i++)
                Assert.Equal(expected[i], output[i]);
        }

        [Fact]
        public void IntegerRLE_Read_Direct()
        {
            Span<byte> input = stackalloc byte[] { 0x5e, 0x03, 0x5c, 0xa1, 0xab, 0x1e, 0xde, 0xad, 0xbe, 0xef };
            Span<long> expected = stackalloc long[] { 23713, 43806, 57005, 48879 };
            Span<long> output = stackalloc long[4];

            var bufferReader = new BufferReader(input);
            var numValuesRead = OptimizedIntegerRLE.ReadValues(ref bufferReader, isSigned: false, output);

            Assert.Equal(expected.Length, numValuesRead);
            for (int i = 0; i < numValuesRead; i++)
                Assert.Equal(expected[i], output[i]);
        }

        [Fact]
        public void IntegerRLE_Read_PatchedBase()
        {
            Span<byte> input = stackalloc byte[] { 0x8e, 0x13, 0x2b, 0x21, 0x07, 0xd0, 0x1e, 0x00, 0x14, 0x70, 0x28, 0x32, 0x3c, 0x46, 0x50, 0x5a, 0x64, 0x6e, 0x78, 0x82, 0x8c, 0x96, 0xa0, 0xaa, 0xb4, 0xbe, 0xfc, 0xe8 };
            Span<long> expected = stackalloc long[] { 2030, 2000, 2020, 1000000, 2040, 2050, 2060, 2070, 2080, 2090, 2100, 2110, 2120, 2130, 2140, 2150, 2160, 2170, 2180, 2190 };
            Span<long> output = stackalloc long[20];

            var bufferReader = new BufferReader(input);
            var numValuesRead = OptimizedIntegerRLE.ReadValues(ref bufferReader, isSigned: false, output);

            Assert.Equal(expected.Length, numValuesRead);
            for (int i = 0; i < numValuesRead; i++)
                Assert.Equal(expected[i], output[i]);
        }

        [Fact]
        public void IntegerRLE_Read_Delta()
        {
            Span<byte> input = stackalloc byte[] { 0xc6, 0x09, 0x02, 0x02, 0x22, 0x42, 0x42, 0x46 };
            Span<long> expected = stackalloc long[] { 2, 3, 5, 7, 11, 13, 17, 19, 23, 29 };
            Span<long> output = stackalloc long[10];

            var bufferReader = new BufferReader(input);
            var numValuesRead = OptimizedIntegerRLE.ReadValues(ref bufferReader, isSigned: false, output);

            Assert.Equal(expected.Length, numValuesRead);
            for (int i = 0; i < numValuesRead; i++)
                Assert.Equal(expected[i], output[i]);
        }

        [Fact]
        public void ByteRLE_100_Zeros()
        {
            Span<byte> input = stackalloc byte[] { 0x61, 0x00 };
            Span<byte> expected = stackalloc byte[100];
            Span<byte> output = stackalloc byte[100];

            var bufferReader = new BufferReader(input);
            var numValuesRead = OptimizedByteRLE.ReadValues(ref bufferReader, output);

            Assert.Equal(expected.Length, numValuesRead);
            for (int i = 0; i < numValuesRead; i++)
                Assert.Equal(expected[i], output[i]);
        }

        [Fact]
        public void ByteRLE_Two_Values()
        {
            Span<byte> input = stackalloc byte[] { 0xfe, 0x44, 0x45 };
            Span<byte> expected = stackalloc byte[] { 68, 69 };
            Span<byte> output = stackalloc byte[2];

            var bufferReader = new BufferReader(input);
            var numValuesRead = OptimizedByteRLE.ReadValues(ref bufferReader, output);

            Assert.Equal(expected.Length, numValuesRead);
            for (int i = 0; i < numValuesRead; i++)
                Assert.Equal(expected[i], output[i]);
        }
    }
}
