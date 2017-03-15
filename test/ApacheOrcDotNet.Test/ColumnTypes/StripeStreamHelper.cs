using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Protocol;
using ApacheOrcDotNet.Stripes;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace ApacheOrcDotNet.Test.ColumnTypes
{
    public static class StripeStreamHelper
    {
		public static void Write<T>(System.IO.Stream outputStream, IEnumerable<T> values, out Footer footer) where T : class
		{
			var bufferFactory = new OrcCompressedBufferFactory(256 * 1024, CompressionKind.Zlib, CompressionStrategy.Size);
			var stripeWriter = new StripeWriter(typeof(T), outputStream, false, 0.8, 18,6, bufferFactory, 10000, 512 * 1024 * 1024);
			stripeWriter.AddRows(values);
			stripeWriter.RowAddingCompleted();
			footer = stripeWriter.GetFooter();

			outputStream.Seek(0, SeekOrigin.Begin);
		}

		public static StripeStreamReaderCollection GetStripeStreams(System.IO.Stream inputStream, Footer footer)
		{
			var stripes = new StripeReaderCollection(inputStream, footer, CompressionKind.Zlib);
			Assert.Equal(1, stripes.Count);
			return stripes[0].GetStripeStreamCollection();
		}
	}
}
