using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using System.Threading.Tasks;
using Xunit;

namespace ApacheOrcDotNet.OptimizedReader.Test.ColumnTypes
{
    public class Buffers_Test : _BaseColumnTypeWithNulls
    {
        [Fact]
        public async Task Small_ByteRange_Buffer_Will_Throw()
        {
            // Maximum byte range requested by the binary column is 22191 bytes.
            var config = new OrcReaderConfiguration() { StreamDecompressionBufferLength = 1024 };
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("binary");
            var columnBuffer = reader.CreateBinaryColumnBuffer(column);

            var exception = await Assert.ThrowsAsync<CompressionBufferException>(async () =>
                await reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 0, columnBuffer)
            );

            Assert.NotNull(exception);
            Assert.Contains("22191", exception.Message);
            Assert.Contains("1024", exception.Message);
        }

        [Fact]
        public async Task Small_Decompress_Buffer_Will_Throw()
        {
            // Maximum decompressed length required by the binary column is 68776 bytes.
            var config = new OrcReaderConfiguration() { StreamDecompressionBufferLength = 32768 };
            var reader = new OrcReader(config, _byteRangeProvider);

            var column = reader.GetColumn("binary");
            var columnBuffer = reader.CreateBinaryColumnBuffer(column);

            var exception = await Assert.ThrowsAsync<CompressionBufferException>(async () =>
                await reader.LoadDataAsync(stripeId: 0, rowEntryIndexId: 0, columnBuffer)
            );

            Assert.NotNull(exception);
            Assert.Contains("68776", exception.Message);
            Assert.Contains("32768", exception.Message);
        }

        [Fact]
        public void Tail_Decompress_Buffer_Will_Throw()
        {
            // Initial decompressed length required by the tail section is 523 bytes.
            var config = new OrcReaderConfiguration() { MetadataDecompressionBufferLength = 512 };

            var exception = Assert.Throws<CompressionBufferException>(() =>
            {
                var reader = new OrcReader(config, _byteRangeProvider);
            });

            Assert.NotNull(exception);
            Assert.Contains("523", exception.Message);
            Assert.Contains("512", exception.Message);
        }
    }
}
