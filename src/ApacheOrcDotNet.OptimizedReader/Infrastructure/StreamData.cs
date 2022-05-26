using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Protocol;
using System;

namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public static class StreamData
    {
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
                    ? inputBuffer.Slice(inputPosition)
                    : inputBuffer.Slice(inputPosition, compressedChunkLength);

                outputPosition += OrcCompressedBlock.DecompressBlock(compressionKind, chunkToDecompress, outputBuffer.Slice(outputPosition));

                inputPosition += compressedChunkLength;
            }

            return outputPosition;
        }
    }
}
