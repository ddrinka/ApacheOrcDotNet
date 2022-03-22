using ApacheOrcDotNet.Compression;
using ProtoBuf;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
            int decompressedBufferLength = OrcCompressedBlock.PeekLength(postScript.Compression, buffer[footerStart..]);
            var compressedFooter = buffer.Slice(footerStart, (int)postScript.FooterLength);
            var decompressedFooter = ArrayPool<byte>.Shared.Rent(decompressedBufferLength);
            OrcCompressedBlock.DecompressBlock(postScript.Compression, compressedFooter, decompressedFooter);
            var footer = Serializer.Deserialize<Protocol.Footer>(decompressedFooter.AsSpan()[..decompressedBufferLength]);
            ArrayPool<byte>.Shared.Return(decompressedFooter);

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
