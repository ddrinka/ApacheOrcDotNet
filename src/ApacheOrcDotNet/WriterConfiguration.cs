using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet
{
	public enum CompressionType { None, ZLIB }
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
		/// If more than this fraction of keys in a dictionary are unique, disable dictionary encoding
		/// </summary>
		public double DictionaryKeySizeThreshold { get; set; } = 0.8;

		/// <summary>
		/// Default precision (total number of digits before and after the decimal place) to use for decimals with un-configured precision
		/// The default, 18, allows Presto to read decimals as longs rather than BigInts
		/// </summary>
		public int DefaultDecimalPrecision { get; set; } = 18;

		/// <summary>
		/// Default scale (number of digits after the decimal place) to use for decimals with un-configured precision
		/// The default, 6, allows for number to be stored from -999,999,999,999.999999 to 999,999,999,999.999999, i.e. approximately one trillion
		/// </summary>
		public int DefaultDecimalScale { get; set; } = 6;
	}
}
