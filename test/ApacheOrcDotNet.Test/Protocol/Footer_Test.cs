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
			var postscriptBytes = helper.GetPostscriptBytes(postscriptLength);
			var postscriptStream = new MemoryStream(postscriptBytes);
			var postScript = Serializer.Deserialize<PostScript>(postscriptStream);
			var footerLength = postScript.FooterLength;
			var footerBytesCompressed = helper.GetFooterRawBytes(postscriptLength, footerLength);
			var footerBytesDecompressed = new byte[postScript.CompressionBlockSize];    //Don't do this in production
			helper.ZLibDecompress(footerBytesCompressed, footerBytesDecompressed);
			var footerStream = new MemoryStream(footerBytesDecompressed);
			var footer = Serializer.Deserialize<Footer>(footerStream);


		}
	}
}
