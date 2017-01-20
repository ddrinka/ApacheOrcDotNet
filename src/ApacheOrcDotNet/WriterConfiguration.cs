using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet
{
	public enum CompressionType { ZLIB, Snappy, LZO, LZ4 }
	public enum Version { V0_12 }
	public enum EncodingStrategy { Speed, Size }
	public enum CompressionStrategy { Speed, Size }
	public enum BloomFilterVersion { Original, UTF8 }

    public class WriterConfiguration
    {
		/// <summary>
		/// Stripe size, in bytes
		/// </summary>
		public long StripeSize { get; set; } = 64L * 1024 * 1024;

		/// <summary>
		/// Block size
		/// </summary>
		public long BlockSize { get; set; } = 256L * 1024 * 1024;

		/// <summary>
		/// Should indexes be created
		/// </summary>
		public bool EnableIndexes { get; set; } = true;

		/// <summary>
		/// Index stride, in rows
		/// </summary>
		public int RowIndexStride { get; set; } = 10000;

		/// <summary>
		/// Buffer size, in bytes
		/// </summary>
		public int BufferSize { get; set; } = 256 * 1024;

		/// <summary>
		/// Compression codec to utilize
		/// </summary>
		public CompressionType Compress { get; set; } = CompressionType.ZLIB;

		/// <summary>
		/// Version of the file to write
		/// </summary>
		public Version WriteFormat { get; set; } = Version.V0_12;

		/// <summary>
		/// Adjusts the light-weight encoding of integers
		/// </summary>
		public EncodingStrategy EncodingStrategy { get; set; } = EncodingStrategy.Speed;

		/// <summary>
		/// Changes compression level of chosen compressor
		/// </summary>
		public CompressionStrategy CompressionStrategy { get; set; } = CompressionStrategy.Speed;

		/// <summary>
		/// False positive probability for bloom filters
		/// </summary>
		public double BloomFilterFpp { get; set; } = 0.05;

		/// <summary>
		/// If this fraction of keys in a dictionary are unique, disable dictionary encoding
		/// </summary>
		public double DictionaryKeySizeThreshold { get; set; } = 0.8;

		/// <summary>
		/// Check the DictionaryKeySizeThreshold after the first index stride, rather than after the first stripe
		/// </summary>
		public bool RowIndexStrideDictionaryCheck { get; set; } = true;

		/// <summary>
		/// List of columns to create bloom filters for
		/// </summary>
		public string BloomFilterColumns { get; set; } = "";

		/// <summary>
		/// Search argument for predicate pushdown
		/// </summary>
		public string KryoSarg { get; set; } = null;

		/// <summary>
		/// Column names for the search argument
		/// </summary>
		public string SargColumns { get; set; } = null;
	}
}
