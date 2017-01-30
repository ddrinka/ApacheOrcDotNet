using ApacheOrcDotNet.Compression;
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
			var postscriptStream = helper.GetPostscriptStream(postscriptLength);
			var postScript = Serializer.Deserialize<PostScript>(postscriptStream);
			var footerLength = postScript.FooterLength;
			var metadataLength = postScript.MetadataLength;
			var metadataStreamCompressed = helper.GetMetadataCompressedStream(postscriptLength, footerLength, metadataLength);
			var metadataStream = OrcCompressedStream.GetDecompressingStream(metadataStreamCompressed, CompressionKind.Zlib);
			var metadata = Serializer.Deserialize<Metadata>(metadataStream);

			Assert.Equal(1, metadata.StripeStats.Count);
			Assert.Equal(10, metadata.StripeStats[0].ColStats.Count);
		}
	}
}
