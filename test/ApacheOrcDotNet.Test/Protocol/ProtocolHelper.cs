using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Test.TestHelpers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Test.Protocol
{
    public class ProtocolHelper
    {
		readonly DataFileHelper _dataFileHelper;
		readonly ZLib _zlib = new ZLib(CompressionStrategy.Speed);

		public ProtocolHelper(string dataFileName)
		{
			_dataFileHelper = new DataFileHelper(dataFileName);
		}

		public int GetPostscriptLength()
		{
			var lastByte = _dataFileHelper.Read(_dataFileHelper.Length - 1, 1)[0];
			return lastByte;
		}

		public byte[] GetPostscriptBytes(int postscriptLength)
		{
			var offset = _dataFileHelper.Length - 1 - postscriptLength;
			var bytes = _dataFileHelper.Read(offset, postscriptLength);
			return bytes;
		}

		public byte[] GetFooterRawBytes(int postscriptLength, ulong footerLength)
		{
			var offset = _dataFileHelper.Length - 1 - postscriptLength - (long)footerLength;
			var bytes = _dataFileHelper.Read(offset, (int)footerLength);
			return bytes;
		}

		public void ZLibDecompress(byte[] compressedData, byte[] decompressedData)
		{
			_zlib.Decompress(compressedData, decompressedData);
		}
    }
}
