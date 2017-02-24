using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.FileProviders;
using ApacheOrcDotNet.Infrastructure;

namespace ApacheOrcDotNet.Test.TestHelpers
{
    public class DataFileHelper : IDisposable
    {
		readonly Stream _dataStream;
		public DataFileHelper(string dataFileName)
		{
			var embeddedFileName = $"Data.{dataFileName}";

			var fileProvider = new EmbeddedFileProvider(typeof(DataFileHelper).GetTypeInfo().Assembly);
			var fileInfo = fileProvider.GetFileInfo(embeddedFileName);
			if (!fileInfo.Exists)
				throw new ArgumentException("Requested data file doesn't exist");

			_dataStream = fileInfo.CreateReadStream();
		}

		public DataFileHelper(Stream inputStream)
		{
			_dataStream = inputStream;
		}

		public void Dispose()
		{
			_dataStream.Dispose();
		}

		public long Length => _dataStream.Length;
		public byte[] Read(long fileOffset, int length)
		{
			var buffer = new byte[length];
			_dataStream.Seek(fileOffset, SeekOrigin.Begin);
			var readLen = _dataStream.Read(buffer, 0, length);
			if (readLen != length)
				throw new InvalidOperationException("Read returned less data than requested");

			return buffer;
		}

		public Stream GetStreamSegment(long fileOffset, ulong length)
		{
			_dataStream.Seek(fileOffset, SeekOrigin.Begin);
			return new StreamSegment(_dataStream, (long)length, true);
		}

		public Stream GetStream()
		{
			return _dataStream;
		}
	}
}
