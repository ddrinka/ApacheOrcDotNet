using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Protocol;
using System;

namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public static class CompressedData
    {
        public static byte[] CheckDecompressionBuffer(ReadOnlySpan<byte> inputBuffer, byte[] targetDecompressionBuffer, CompressionKind compressionKind, int maxDecompressedLengthPerChunk)
        {
            int inputPosition = 0;
            int maxDecompressionLength = 0;
            while (inputPosition < inputBuffer.Length)
            {
                var compressedChunkLength = OrcCompressedBlock.GetChunkLength(compressionKind, inputBuffer[inputPosition..]);
                inputPosition += compressedChunkLength;
                maxDecompressionLength += maxDecompressedLengthPerChunk;
            }

            if (maxDecompressionLength > targetDecompressionBuffer.Length)
            {
                Console.WriteLine("Aaaaa");
                targetDecompressionBuffer = new byte[maxDecompressionLength];
            }


            return targetDecompressionBuffer;
        }

        public static byte[] CreateDecompressionBuffer(ReadOnlySpan<byte> inputBuffer, CompressionKind compressionKind, int maxDecompressedLengthPerChunk)
            => CheckDecompressionBuffer(inputBuffer, new byte[maxDecompressedLengthPerChunk], compressionKind, maxDecompressedLengthPerChunk);

        public static int Decompress(ReadOnlySpan<byte> inputBuffer, Span<byte> outputBuffer, CompressionKind compressionKind)
        {
            if (inputBuffer.IsEmpty)
                return 0;

            int inputPosition = 0;
            int outputPosition = 0;
            while (inputPosition < inputBuffer.Length)
            {
                var compressedChunkLength = OrcCompressedBlock.GetChunkLength(compressionKind, inputBuffer[inputPosition..]);

                var chunkToDecompress = (inputPosition + compressedChunkLength) > inputBuffer.Length
                    ? inputBuffer[inputPosition..]
                    : inputBuffer.Slice(inputPosition, compressedChunkLength);

                outputPosition += OrcCompressedBlock.DecompressBlock(compressionKind, chunkToDecompress, outputBuffer[outputPosition..]);

                inputPosition += compressedChunkLength;
            }

            return outputPosition;
        }
    }
}
