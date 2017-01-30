using ApacheOrcDotNet.Compression;
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
			var footerStream = OrcCompressedStream.GetDecompressingStream(footerStreamCompressed, CompressionKind.Zlib);
			var footer = Serializer.Deserialize<Footer>(footerStream);

			var stripeDetails = footer.Stripes[0];
			var streamFooterStreamCompressed = helper.GetStripeFooterCompressedStream(stripeDetails.Offset, stripeDetails.IndexLength, stripeDetails.DataLength, stripeDetails.FooterLength);
			var stripeFooterStream = OrcCompressedStream.GetDecompressingStream(streamFooterStreamCompressed, CompressionKind.Zlib);
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
						var dataStream = OrcCompressedStream.GetDecompressingStream(dataStreamCompressed, CompressionKind.Zlib);
						var reader = new IntegerRunLengthEncodingV2Reader(dataStream, true);
						var result = reader.Read().ToArray();

						for(int i=0;i<result.Length;i++)
						{
							if (stream.Column == 1)
							{
								var expected = i + 1;
								Assert.Equal(expected, result[i]);
							}
							else if (stream.Column == 5)
							{
								var expected = ((i / 70) * 500) % 10000 + 500;
								Assert.Equal(expected, result[i]);
							}
							else if (stream.Column == 7)
							{
								var expected = (i / 5600) % 7;
								Assert.Equal(expected, result[i]);
							}
							else if (stream.Column == 8)
							{
								var expected = (i / 39200) % 7;
								Assert.Equal(expected, result[i]);
							}
							else if (stream.Column == 9)
							{
								var expected = (i / 274400);
								Assert.Equal(expected, result[i]);
							}
							else
								Assert.True(false, "Unexpected column");
						}
					}
				}

				offset += stream.Length;
			}
		}
    }
}
