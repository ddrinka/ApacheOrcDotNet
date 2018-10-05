using System.Collections.Generic;
using System.IO;
using ApacheOrcDotNet.FluentSerialization;

namespace ApacheOrcDotNet
{
	public class OrcWriter<T> : IOrcWriter<T>
	{
		readonly OrcWriter _underlyingOrcWriter;

		public OrcWriter(Stream outputStream, WriterConfiguration configuration, SerializationConfiguration serializationConfiguration = null)
		{
			_underlyingOrcWriter = new OrcWriter(typeof(T), outputStream, configuration, serializationConfiguration);
		}

		public void AddRow(T row) => _underlyingOrcWriter.AddRow(row);
		public void AddRows(IEnumerable<T> rows) => _underlyingOrcWriter.AddRows((IEnumerable<object>)rows);
		public void AddUserMetadata(string key, byte[] value) => _underlyingOrcWriter.AddUserMetadata(key, value);
		public void Dispose() => _underlyingOrcWriter.Dispose();
	}
}
