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
		public void SingleStreamSingleStrideSingleBlock()
		{
			RoundTripSingleInt(100);
		}

		[Fact]
		public void SingleStreamMultiStrideMultiBlock()
		{
			RoundTripSingleInt(70000);	//70,000*4 > 256*1024 and 70,000 > 10,000
		}

		void RoundTripSingleInt(int numValues)
		{
			var pocos = new List<SingleValuePoco>();
			var random = new Random(123);
			for (int i = 0; i < numValues; i++)
				pocos.Add(new SingleValuePoco { IntProperty1 = random.Next() });

			var bufferFactory = new OrcCompressedBufferFactory(256 * 1024, CompressionKind.Zlib, CompressionStrategy.Size);
			var stream = new MemoryStream();

			var stripeWriter = new StripeWriter(typeof(SingleValuePoco), stream, false, bufferFactory, 10000, 512 * 1024 * 1024);
			stripeWriter.AddRows(pocos);
			stripeWriter.RowAddingCompleted();
			var footer=stripeWriter.GetFooter();

			stream.Seek(0, SeekOrigin.Begin);
			var stripes = new StripeReaderCollection(stream, footer, CompressionKind.Zlib);
			Assert.Equal(1, stripes.Count);
			Assert.Equal((ulong)numValues, stripes[0].NumRows);
			var stripeStreams = stripes[0].GetStripeStreamCollection();
			var longReader = new LongReader(stripeStreams, 1);
			var results = longReader.Read().ToArray();

			for (int i = 0; i < numValues; i++)
				Assert.Equal(pocos[i].IntProperty1, results[i]);
		}
	}

	class SingleValuePoco
	{
		public int IntProperty1 { get; set; }
	}
}
