using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader
{
    public static class ByteRangeProviderExtensions
    {
        /// <summary>
        /// Retrieves data from byte range, decompresses it, provides it to the parser, and then releases it
        /// </summary>
        /// <remarks>
        /// Any data returned from the parser must be a copy and not a reference to the input bytes
        /// </remarks>
        public static TResult DecompressAndParseByteRange<TResult>(this IByteRangeProvider provider, long offset, int length, Protocol.CompressionKind compressionKind, int compressionBlockSize, Func<ReadOnlySequence<byte>, TResult> parser)
        {
            var buffer = ArrayPool<byte>.Shared.Rent(length);
            TResult result;
            try
            {
                var bufferSpan = buffer.AsSpan()[..length];
                provider.GetRange(bufferSpan, (int)offset);     //TODO support > 2TB files
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
