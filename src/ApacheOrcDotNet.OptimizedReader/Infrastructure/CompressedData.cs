using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Protocol;
using System;
using System.Buffers;

namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public static class CompressedData
    {
        public static int Decompress(ReadOnlySpan<byte> inputBuffer, Span<byte> outputBuffer, CompressionKind compressionKind, ulong decompressionBufferLength)
        {
            if (inputBuffer.IsEmpty)
                return 0;

            int inputPosition = 0;
            int outputPosition = 0;
            var decompressionBuffer = ArrayPool<byte>.Shared.Rent(checked((int)decompressionBufferLength));

            try
            {
                while (inputPosition < inputBuffer.Length)
                {
                    var compressedChunkLength = OrcCompressedBlock.GetChunkLength(compressionKind, inputBuffer[inputPosition..]);

                    var chunkToDecompress = (inputPosition + compressedChunkLength) > inputBuffer.Length
                        ? inputBuffer.Slice(inputPosition)
                        : inputBuffer.Slice(inputPosition, compressedChunkLength);

                    var numDecompressBytes = OrcCompressedBlock.DecompressBlock(compressionKind, chunkToDecompress, decompressionBuffer.AsSpan());

                    if (outputPosition + numDecompressBytes >= outputBuffer.Length)
                        throw new CompressionBufferException(nameof(outputBuffer), outputBuffer.Length, outputPosition + numDecompressBytes);

                    decompressionBuffer.AsSpan().Slice(0, numDecompressBytes).CopyTo(outputBuffer.Slice(outputPosition));

                    outputPosition += numDecompressBytes;
                    inputPosition += compressedChunkLength;
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(decompressionBuffer);
            }

            return outputPosition;
        }
    }
}
