using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Compression
{
    public interface ICompressor
    {
		/// <summary>
		/// Compress a block of data
		/// </summary>
		/// <param name="inputBuffer">Input data</param>
		/// <param name="offset">Offset to begin compressing at</param>
		/// <returns>Compressed data</returns>
		byte[] Compress(byte[] inputBuffer, int offset);
    }
}
