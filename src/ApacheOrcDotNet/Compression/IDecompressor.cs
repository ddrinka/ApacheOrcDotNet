using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Compression
{
    public interface IDecompressor
    {
		/// <summary>
		/// Decompress a block of data
		/// </summary>
		/// <param name="compressedData">Compressed Data</param>
		/// <param name="offset">Offset to begin decompressing at</param>
		/// <returns>Decompressed data</returns>
		byte[] Decompress(byte[] compressedData, int offset);
	}
}
