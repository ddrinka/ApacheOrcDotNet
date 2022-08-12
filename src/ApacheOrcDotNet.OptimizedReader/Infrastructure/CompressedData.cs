using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Protocol;
using System;

namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public static class CompressedData
    {
        public static int GetRequiredBufferSize(ReadOnlySpan<byte> inputBuffer, CompressionKind compressionKind, int maxDecompressedLengthPerChunk)
        {
            int inputPosition = 0;
            int maxDecompressionLength = 0;
            while (inputPosition < inputBuffer.Length)
            {
                var compressedChunkLength = OrcCompressedBlock.GetChunkLength(compressionKind, inputBuffer[inputPosition..]);
                inputPosition += compressedChunkLength;
                maxDecompressionLength += maxDecompressedLengthPerChunk;
            }

            return maxDecompressionLength;
        }

        public static byte[] CreateDecompressionBuffer(ReadOnlySpan<byte> inputBuffer, CompressionKind compressionKind, int maxDecompressedLengthPerChunk)
        { 
            var bufferSize = GetRequiredBufferSize(inputBuffer, compressionKind, maxDecompressedLengthPerChunk);

            return new byte[bufferSize];
        }

        public static int Decompress(ReadOnlySpan<byte> inputBuffer, Span<byte> outputBuffer, CompressionKind compressionKind)
        {
            if (inputBuffer.IsEmpty)
                return 0;

            int inputPosition = 0;
            int outputPosition = 0;
            while (inputPosition < inputBuffer.Length)
            {
                var compressedChunkLength = OrcCompressedBlock.GetChunkLength(compressionKind, inputBuffer[inputPosition..]);

                outputPosition += OrcCompressedBlock.DecompressBlock(compressionKind, inputBuffer.Slice(inputPosition, compressedChunkLength), outputBuffer[outputPosition..]);

                inputPosition += compressedChunkLength;
            }

            return outputPosition;
        }
    }
}
