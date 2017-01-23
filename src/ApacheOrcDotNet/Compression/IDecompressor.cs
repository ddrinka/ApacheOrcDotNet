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
		/// <param name="inputBuffer">Compressed Data</param>
		/// <param name="outputBuffer">Decompressed Data</param>
		void Decompress(byte[] inputBuffer, byte[] outputBuffer);
	}
}
