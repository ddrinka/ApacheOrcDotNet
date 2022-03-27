using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ProtoBuf;
using System;
using System.IO;

namespace ApacheOrcDotNet.OptimizedReader
{
    public sealed class SpanFileTail
    {
        public Protocol.PostScript PostScript { get; private init; }
        public Protocol.Footer Footer { get; private init; }

        public static bool TryRead(ReadOnlySpan<byte> buffer, out SpanFileTail fileTail, out int additionalBytesRequired)
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
            if (buffer.Length < accumulatedLength)
            {
                additionalBytesRequired = accumulatedLength - buffer.Length;
                fileTail = null;
                return false;
            }

            int footerStart = buffer.Length - accumulatedLength;
            var compressedFooter = buffer.Slice(footerStart, (int)postScript.FooterLength);
            using var decompressedMemorySequence = new DecompressingMemorySequence(compressedFooter, postScript.Compression, (int)postScript.CompressionBlockSize);
            var footer = Serializer.Deserialize<Protocol.Footer>(decompressedMemorySequence.Sequence);

            fileTail = new SpanFileTail
            {
                PostScript = postScript,
                Footer = footer
            };
            additionalBytesRequired = 0;
            return true;
        }
    }
}
