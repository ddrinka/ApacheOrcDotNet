using ApacheOrcDotNet.ColumnTypes;
using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.Protocol;
using ApacheOrcDotNet.Stripes;
using ApacheOrcDotNet.Test.Protocol;
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace ApacheOrcDotNet.Test.Stripes
{
	public class StripeWriter_Test
	{
		[Fact]
		public void SingleStreamSingleBlock()
		{
			var pocos = new List<SingleValuePoco>();
			for (int i = 0; i < 100; i++)
				pocos.Add(new SingleValuePoco { IntProperty1 = i });

			var bufferFactory = new OrcCompressedBufferFactory(256 * 1024, CompressionKind.Zlib, CompressionStrategy.Size);
			var stream = new MemoryStream();
			var footer = new Footer();

			var stripeWriter = new StripeWriter(typeof(SingleValuePoco), stream, false, bufferFactory, 10000, 512 * 1024 * 1024);
			stripeWriter.AddRows(pocos);
			stripeWriter.RowAddingCompleted();
			stripeWriter.FillFooter(footer);

			stream.Seek(0, SeekOrigin.Begin);
			var stripes = new StripeReaderCollection(stream, footer, CompressionKind.Zlib);
			Assert.Equal(1, stripes.Count);
			Assert.Equal(100ul, stripes[0].NumRows);
			var stripeStreams = stripes[0].GetStripeStreamCollection();
			var longReader = new LongReader(stripeStreams, 0);
			var results = longReader.Read().ToArray();

			for (int i = 0; i < 100; i++)
				Assert.Equal(i, results[i]);
		}
	}

	class SingleValuePoco
	{
		public int IntProperty1 { get; set; }
	}
}
