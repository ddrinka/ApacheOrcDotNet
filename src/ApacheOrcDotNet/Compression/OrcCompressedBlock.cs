using ApacheOrcDotNet.Protocol;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;

namespace ApacheOrcDotNet.Compression
{
    public static class OrcCompressedBlock
    {
        public static int PeekLength(CompressionKind compressionKind, ReadOnlySpan<byte> input)
        {
            if (compressionKind == CompressionKind.None)
                return input.Length;

            ReadBlockHeader(input, out var blockLength, out _);
            return blockLength;
        }

        private static void ReadBlockHeader(ReadOnlySpan<byte> input, out int blockLength, out bool isCompressed)
        {
            if (input.Length < 3)
                throw new InvalidDataException("Compression block header requires three bytes");

            var rawValue = input[0] | input[1] << 8 | input[2] << 16;
            blockLength = rawValue >> 1;
            isCompressed = (rawValue & 1) == 0;
        }

        public unsafe static int DecompressBlock(CompressionKind compressionKind, ReadOnlySpan<byte> input, Span<byte> output)
        {
            if (compressionKind == CompressionKind.None)
            {
                input.CopyTo(output);
                return input.Length;
            }

            ReadBlockHeader(input, out var blockLength, out var isCompressed);
            if (!isCompressed)
            {
                input.Slice(3, blockLength).CopyTo(output);
                return blockLength;
            }

            if (compressionKind != CompressionKind.Zlib)
                throw new NotSupportedException("Only Zlib compression is currently supported");

            //.Net 5 only provides a stream-based ZLib inflator.  .Net 7 will likely introduce low-level calls to make this more optimized
            //https://github.com/dotnet/runtime/issues/62113
            fixed (byte* pBuffer = &input[0])
            {
                using var stream = new UnmanagedMemoryStream(pBuffer, input.Length);
                using var deflateStream = new DeflateStream(stream, CompressionMode.Decompress);
                deflateStream.Read(output.Slice(blockLength));
            }

            return blockLength;
        }
    }
}
