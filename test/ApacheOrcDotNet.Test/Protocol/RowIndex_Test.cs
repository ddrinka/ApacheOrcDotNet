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
    public class RowIndex_Test
    {
		[Fact]
		public void RowIndex_ShouldMatchExpected()
		{
			var helper = new ProtocolHelper("demo-12-zlib.orc");
			var postscriptLength = helper.GetPostscriptLength();
			var postscriptBytes = helper.GetPostscriptBytes(postscriptLength);
			var postscriptStream = new MemoryStream(postscriptBytes);
			var postScript = Serializer.Deserialize<PostScript>(postscriptStream);
			var footerLength = postScript.FooterLength;
			var footerBytesCompressed = helper.GetFooterRawBytes(postscriptLength, footerLength);
			var footerBytes = helper.DecompressBlock(footerBytesCompressed);
			var footerStream = new MemoryStream(footerBytes);
			var footer = Serializer.Deserialize<Footer>(footerStream);

			var stripeDetails = footer.Stripes[0];
			var stripeFooterBytesCompressed = helper.GetStripeFooterRawBytes(stripeDetails.Offset, stripeDetails.IndexLength, stripeDetails.DataLength, stripeDetails.FooterLength);
			var stripeFooterBytes = helper.DecompressBlock(stripeFooterBytesCompressed);
			var stripeFooterStream = new MemoryStream(stripeFooterBytes);
			var stripeFooter = Serializer.Deserialize<StripeFooter>(stripeFooterStream);

			var offset = stripeDetails.Offset;
			foreach(var stream in stripeFooter.Streams)
			{
				if(stream.Kind==StreamKind.RowIndex)
				{
					var rowIndexBytesCompressed = helper.GetRowIndexBytes(offset, stream.Length);
					var rowIndexBytes = helper.DecompressBlock(rowIndexBytesCompressed);
					var rowIndexStream = new MemoryStream(rowIndexBytes);
					var rowIndex = Serializer.Deserialize<RowIndex>(rowIndexStream);
				}

				offset += stream.Length;
			}
		}
	}
}
