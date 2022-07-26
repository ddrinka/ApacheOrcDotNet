﻿using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using System;
using System.Threading.Tasks;
using Xunit;

namespace ApacheOrcDotNet.OptimizedReader.Test.ColumnTypes
{
    public class _Buffers_Test : _BaseColumnTypeWithNulls
    {
        [Fact]
        public async Task Small_ByteRange_Buffer_Will_Throw()
        {
            // Maximum byte range requested by the binary column is 22191 bytes.
            var config = new OrcReaderConfiguration() { DecompressionBufferLength = 1024 };
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("binary");
            var columnBuffer = reader.CreateBinaryColumnBuffer(column);

            try
            {
                await reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 0, columnBuffer);
            }
            catch (Exception ex)
            {
                Assert.True(ex is CompressionBufferException);
                Assert.Contains("22191", ex.Message);
                Assert.Contains("1024", ex.Message);
            }
        }

        [Fact]
        public async Task Small_Decompress_Buffer_Will_Throw()
        {
            // Maximum decompressed length required by the binary column is 68776 bytes.
            var config = new OrcReaderConfiguration() { DecompressionBufferLength = 32768 };
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("binary");
            var columnBuffer = reader.CreateBinaryColumnBuffer(column);

            try
            {
                await reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 0, columnBuffer);
            }
            catch (Exception ex)
            {
                Assert.True(ex is CompressionBufferException);
                Assert.Contains(nameof(CompressedData.Decompress), ex.Message);
                Assert.Contains("68776", ex.Message);
                Assert.Contains("32768", ex.Message);
            }
        }
    }
}
