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
    public class Footer_Test
    {
		[Fact]
		public void Footer_ShouldMatchExpected()
		{
			var helper = new ProtocolHelper("demo-12-zlib.orc");
			var postscriptLength = helper.GetPostscriptLength();
			var postscriptStream = helper.GetPostscriptStream(postscriptLength);
			var postScript = Serializer.Deserialize<PostScript>(postscriptStream);
			var footerLength = postScript.FooterLength;
			var footerStreamCompressed = helper.GetFooterCompressedStream(postscriptLength, footerLength);
			var footerStream = OrcCompressedStream.GetDecompressingStream(footerStreamCompressed, CompressionKind.Zlib);
			var footer = Serializer.Deserialize<Footer>(footerStream);

			Assert.Equal(1920800ul, footer.NumberOfRows);
			Assert.Equal(1, footer.Stripes.Count);
			Assert.Equal(45592ul, footer.ContentLength);
			Assert.Equal(10000u, footer.RowIndexStride);

			Assert.Equal(1920800ul, footer.Stripes[0].NumberOfRows);
			Assert.Equal(3ul, footer.Stripes[0].Offset);
			Assert.Equal(14035ul, footer.Stripes[0].IndexLength);
			Assert.Equal(31388ul, footer.Stripes[0].DataLength);
			Assert.Equal(166ul, footer.Stripes[0].FooterLength);
		}
	}
}
