using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet
{
    public interface IOrcWriterFactory
    {
		/// <summary>
		/// Create an ORC Writer
		/// </summary>
		/// <typeparam name="T">Data type of rows to be written</typeparam>
		/// <param name="outputStream">Stream to write encoded and compressed data to</param>
		/// <param name="configuration">Configuration for writer</param>
		/// <returns></returns>
		IOrcWriter<T> CreateWriter<T>(Stream outputStream, WriterConfiguration configuration);
		IOrcWriter CreateWriter(Type type, Stream outputStream, WriterConfiguration configuration);
    }
}
