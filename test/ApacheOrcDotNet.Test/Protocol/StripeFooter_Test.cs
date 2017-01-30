using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Protocol;
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace ApacheOrcDotNet.Test.Protocol
{
    public class StripeFooter_Test
    {
		[Fact]
		void StripeFooter_ShouldMatchExpected()
		{
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

			Assert.Equal(10, stripeFooter.Columns.Count);
			Assert.Equal(27, stripeFooter.Streams.Count);
		}
    }
}
