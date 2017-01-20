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
		/// <param name="outputBuffer">Compressed data</param>
		/// <param name="overflow">Data that does not fit in the outputBuffer</param>
		/// <returns>True if the output data size is smaller than the input data size</returns>
		bool Compress(byte[] inputBuffer, byte[] outputBuffer, byte[] overflow);
    }
}
