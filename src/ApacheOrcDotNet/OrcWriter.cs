using ApacheOrcDotNet.Compression;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet
{
	public class OrcWriter<T> : IOrcWriter<T>
	{
		readonly WriterConfiguration _configuration;
//		readonly TreeWriter _treeWriter;

		internal OrcWriter(Stream outputStream, WriterConfiguration configuration)//, ICompressionFactory compressionFactory)
		{
			_configuration = configuration;
//			_compressor = compressionFactory.CreateCompressor(configuration.Compress, configuration.CompressionStrategy);
//			_treeWriter = new TreeWriter(typeof(T), outputStream);  //Which stream???
			configuration.BufferSize = Math.Min(configuration.BufferSize, GetMinimumBufferSize());

//			WriteHeader();
		}

		int GetMinimumBufferSize()
		{
			throw new NotImplementedException();
			//From Java implementation, the recomendation is 2 streams per column with 10 buffers per stream
/*			var numColumns = _treeWriter.NumColumns;
			var desiredBufferSize = (int)(_configuration.StripeSize / (2 * 10 * numColumns));
			for(int i=2;i<7;i++)
			{
				int alignedBufferSize = (int)Math.Pow(2, i) * 1024;
				if (desiredBufferSize < alignedBufferSize)
					return alignedBufferSize;
			}
			return 256 * 1024;
*/
		}

		public void AddRow(T row)
		{
			throw new NotImplementedException();
		}

		public void AddRows(IEnumerable<T> rows)
		{
			throw new NotImplementedException();
		}

		public void AddRows(T[] rows)
		{
			throw new NotImplementedException();
		}

		public void AddUserMetadata(string key, byte[] value)
		{
			throw new NotImplementedException();
		}

		public void Dispose()
		{
			throw new NotImplementedException();
		}
	}
}
