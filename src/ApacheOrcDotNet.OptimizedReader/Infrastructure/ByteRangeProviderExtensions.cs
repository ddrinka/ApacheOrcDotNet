using System;
using System.Buffers;

namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public static class ByteRangeProviderExtensions
    {
        public static DecompressingMemorySequence DecompressByteRange(this IByteRangeProvider provider, long offset, int compressedLength, Protocol.CompressionKind compressionKind, int compressionBlockSize)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(compressedLength);

            try
            {
                var bufferSpan = buffer.AsSpan().Slice(0, compressedLength);

                provider.GetRange(bufferSpan, offset);

                return new DecompressingMemorySequence(bufferSpan, compressionKind, compressionBlockSize);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
    }
}
