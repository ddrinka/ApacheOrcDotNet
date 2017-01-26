using ApacheOrcDotNet.Encodings;
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
    public class IntData_Test
    {
		[Fact]
		public void ReadIntData()
		{
			var helper = new ProtocolHelper("demo-12-zlib.orc");
			var postscriptLength = helper.GetPostscriptLength();
			var postscriptStream = helper.GetPostscriptStream(postscriptLength);
			var postScript = Serializer.Deserialize<PostScript>(postscriptStream);
			var footerLength = postScript.FooterLength;
			var footerStreamCompressed = helper.GetFooterCompressedStream(postscriptLength, footerLength);
			var footerStream = helper.GetDecompressingStream(footerStreamCompressed);
			var footer = Serializer.Deserialize<Footer>(footerStream);

			var stripeDetails = footer.Stripes[0];
			var streamFooterStreamCompressed = helper.GetStripeFooterCompressedStream(stripeDetails.Offset, stripeDetails.IndexLength, stripeDetails.DataLength, stripeDetails.FooterLength);
			var stripeFooterStream = helper.GetDecompressingStream(streamFooterStreamCompressed);
			var stripeFooter = Serializer.Deserialize<StripeFooter>(stripeFooterStream);

			var offset = stripeDetails.Offset;
			foreach (var stream in stripeFooter.Streams)
			{
				var columnInFooter = footer.Types[(int)stream.Column];
				var columnInStripe = stripeFooter.Columns[(int)stream.Column];
				if (columnInFooter.Kind == ColumnTypeKind.Int)
				{
					if (stream.Kind == StreamKind.Data)
					{
						Assert.Equal(ColumnEncodingKind.DirectV2, columnInStripe.Kind);

						var dataStreamCompressed = helper.GetDataCompressedStream(offset, stream.Length);
						var dataStream = helper.GetDecompressingStream(dataStreamCompressed);
						var reader = new IntegerRunLengthEncodingV2Reader(dataStream, true);
						var result = reader.Read().ToArray();
					}
				}

				offset += stream.Length;
			}
		}
    }
}
