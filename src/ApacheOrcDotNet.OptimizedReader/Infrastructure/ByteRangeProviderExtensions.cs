using System;
using System.Buffers;

namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public static class ByteRangeProviderExtensions
    {
        public static DecompressingMemorySequence DecompressByteRangeNew(this IByteRangeProvider provider, OrcContext context, StreamDetail stream, in BufferPositions positions)
        {
            if (stream == null)
            {
                // TODO: Remove this temporary hack
                Span<byte> emptyBuffer = stackalloc byte[0];
                return new DecompressingMemorySequence(emptyBuffer, context.CompressionKind, context.CompressionBlockSize);
            }

            var offset = stream.FileOffset + positions.RowGroupOffset;
            var compressedLength = stream.Length - positions.RowGroupOffset;

            var buffer = ArrayPool<byte>.Shared.Rent(compressedLength);

            try
            {
                var bufferSpan = buffer.AsSpan().Slice(0, compressedLength);

                provider.GetRange(bufferSpan, offset);

                return new DecompressingMemorySequence(bufferSpan, context.CompressionKind, context.CompressionBlockSize);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

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
