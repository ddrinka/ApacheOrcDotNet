using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using System;
using System.Buffers;

namespace ApacheOrcDotNet.OptimizedReader
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

        /// <summary>
        /// Retrieves data from byte range, decompresses it, provides it to the parser, and then releases it
        /// </summary>
        /// <remarks>
        /// Any data returned from the parser must be a copy and not a reference to the input bytes
        /// </remarks>
        public static TResult DecompressAndParseByteRange<TResult>(this IByteRangeProvider provider, long offset, int compressedLength, Protocol.CompressionKind compressionKind, int compressionBlockSize, Func<ReadOnlySequence<byte>, TResult> parser)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(compressedLength);
            TResult result;
            try
            {
                var bufferSpan = buffer.AsSpan()[..compressedLength];
                provider.GetRange(bufferSpan, offset);     //TODO support > 2TB files
                using (var decompressed = new DecompressingMemorySequence(bufferSpan, compressionKind, compressionBlockSize))
                {
                    result = parser(decompressed.Sequence);
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }

            return result;
        }
    }
}
