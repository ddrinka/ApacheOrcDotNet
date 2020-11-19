﻿using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Protocol;
using ProtoBuf;
using Xunit;

namespace ApacheOrcDotNet.Test.Protocol {
    public class RowIndex_Test {
        [Fact]
        public void RowIndex_ShouldMatchExpected() {
            var helper = new ProtocolHelper("demo-12-zlib.orc");
            var postscriptLength = helper.GetPostscriptLength();
            var postscriptStream = helper.GetPostscriptStream(postscriptLength);
            var postScript = Serializer.Deserialize<PostScript>(postscriptStream);
            var footerLength = postScript.FooterLength;
            var footerStreamCompressed = helper.GetFooterCompressedStream(postscriptLength, footerLength);
            var footerStream = OrcCompressedStream.GetDecompressingStream(footerStreamCompressed, CompressionKind.Zlib);
            var footer = Serializer.Deserialize<Footer>(footerStream);

            var stripeDetails = footer.Stripes[0];
            var streamFooterStreamCompressed = helper.GetStripeFooterCompressedStream(stripeDetails.Offset, stripeDetails.IndexLength, stripeDetails.DataLength, stripeDetails.FooterLength);
            var stripeFooterStream = OrcCompressedStream.GetDecompressingStream(streamFooterStreamCompressed, CompressionKind.Zlib);
            var stripeFooter = Serializer.Deserialize<StripeFooter>(stripeFooterStream);

            var offset = stripeDetails.Offset;
            foreach (var stream in stripeFooter.Streams) {
                if (stream.Kind == StreamKind.RowIndex) {
                    var rowIndexStreamCompressed = helper.GetRowIndexCompressedStream(offset, stream.Length);
                    var rowIndexStream = OrcCompressedStream.GetDecompressingStream(rowIndexStreamCompressed, CompressionKind.Zlib);
                    var rowIndex = Serializer.Deserialize<RowIndex>(rowIndexStream);
                }

                offset += stream.Length;
            }
        }
    }
}
