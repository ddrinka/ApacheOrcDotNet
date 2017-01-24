using ApacheOrcDotNet.Protocol;
using ApacheOrcDotNet.Test.Protocol;
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace ApacheOrcDotNet.Test.Protocol
{
    public class Metadata_Test
    {
		[Fact]
		public void Metadata_ShouldMatchExpected()
		{
			var helper = new ProtocolHelper("demo-12-zlib.orc");
			var postscriptLength = helper.GetPostscriptLength();
			var postscriptBytes = helper.GetPostscriptBytes(postscriptLength);
			var postscriptStream = new MemoryStream(postscriptBytes);
			var postScript = Serializer.Deserialize<PostScript>(postscriptStream);
			var footerLength = postScript.FooterLength;
			var metadataLength = postScript.MetadataLength;
			var metadataBytesCompressed = helper.GetMetadataRawBytes(postscriptLength, footerLength, metadataLength);
			var metadataBytes = helper.DecompressBlock(metadataBytesCompressed);
			var metadataStream = new MemoryStream(metadataBytes);
			var footer = Serializer.Deserialize<Metadata>(metadataStream);

			Assert.Equal(1, footer.StripeStats.Count);
			Assert.Equal(10, footer.StripeStats[0].ColStats.Count);
		}
	}
}
