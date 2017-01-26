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
	public class PostScript_Test
    {
		[Fact]
		public void PostScript_ShouldMatchExpected()
		{
			var helper = new ProtocolHelper("demo-12-zlib.orc");
			var postscriptLength = helper.GetPostscriptLength();
			var postscriptStream = helper.GetPostscriptStream(postscriptLength);
			var postScript = Serializer.Deserialize<PostScript>(postscriptStream);

			Assert.Equal("ORC", postScript.Magic);
			Assert.Equal(221u, postScript.FooterLength);
			Assert.Equal(CompressionKind.Zlib, postScript.Compression);
			Assert.Equal(262144u, postScript.CompressionBlockSize);
			Assert.Equal(0u, postScript.VersionMajor.Value);
			Assert.Equal(12u, postScript.VersionMinor.Value);
			Assert.Equal(140u, postScript.MetadataLength);
			Assert.Equal(1u, postScript.WriterVersion);
		}
	}
}
