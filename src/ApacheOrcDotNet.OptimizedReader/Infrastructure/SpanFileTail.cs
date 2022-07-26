using ProtoBuf;
using System;
using System.Buffers;
using System.IO;

namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public sealed class SpanFileTail
    {
        public Protocol.PostScript PostScript { get; private init; }
        public Protocol.Footer Footer { get; private init; }
        public Protocol.Metadata Metadata { get; private init; }

        public static bool TryRead(ReadOnlySpan<byte> buffer, int decompressionBufferLength, out SpanFileTail fileTail, out int additionalBytesRequired)
        {
            int accumulatedLength = 1;

            if (buffer.Length < accumulatedLength)
            {
                additionalBytesRequired = accumulatedLength - buffer.Length;
                fileTail = null;
                return false;
            }

            byte postScriptLength = buffer[^1];  //Last byte
            accumulatedLength += postScriptLength;
            if (buffer.Length < accumulatedLength)
            {
                additionalBytesRequired = accumulatedLength - buffer.Length;
                fileTail = null;
                return false;
            }

            int postScriptStart = buffer.Length - accumulatedLength;
            var postScript = Serializer.Deserialize<Protocol.PostScript>(buffer.Slice(postScriptStart, postScriptLength));
            if (postScript.Magic != "ORC")
                throw new InvalidDataException("Postscript didn't contain magic bytes");

            accumulatedLength += (int)postScript.FooterLength;
            int footerStart = buffer.Length - accumulatedLength;
            accumulatedLength += (int)postScript.MetadataLength;
            int metadataStart = buffer.Length - accumulatedLength;

            if (buffer.Length < accumulatedLength)
            {
                additionalBytesRequired = accumulatedLength - buffer.Length;
                fileTail = null;
                return false;
            }

            var compressedFooterBuffer = buffer.Slice(footerStart, (int)postScript.FooterLength);
            var compressedMetadataBuffer = buffer.Slice(metadataStart, (int)postScript.MetadataLength);
            var decompressedFooterBuffer = ArrayPool<byte>.Shared.Rent(decompressionBufferLength);
            var decompressedMetadataBuffer = ArrayPool<byte>.Shared.Rent(decompressionBufferLength);
            var decompressedFooterBufferSpan = decompressedFooterBuffer.AsSpan().Slice(0, decompressionBufferLength);
            var decompressedMetadataBufferSpan = decompressedMetadataBuffer.AsSpan().Slice(0, decompressionBufferLength);

            try
            {
                var decompressedFooterSize = CompressedData.Decompress(compressedFooterBuffer, decompressedFooterBuffer, postScript.Compression, postScript.CompressionBlockSize);
                var footer = Serializer.Deserialize<Protocol.Footer>(decompressedFooterBufferSpan.Slice(0, decompressedFooterSize));

                var decompressedMetadataSize = CompressedData.Decompress(compressedMetadataBuffer, decompressedMetadataBuffer, postScript.Compression, postScript.CompressionBlockSize);
                var metadata = Serializer.Deserialize<Protocol.Metadata>(decompressedMetadataBufferSpan.Slice(0, decompressedMetadataSize));

                fileTail = new SpanFileTail
                {
                    PostScript = postScript,
                    Footer = footer,
                    Metadata = metadata
                };
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(decompressedFooterBuffer);
                ArrayPool<byte>.Shared.Return(decompressedMetadataBuffer);
            }

            additionalBytesRequired = 0;
            return true;
        }
    }
}
