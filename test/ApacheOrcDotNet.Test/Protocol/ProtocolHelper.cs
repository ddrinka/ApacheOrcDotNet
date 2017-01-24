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

		public byte[] DecompressBlock(byte[] inputBytes)
		{
			int blockLength;
			bool isCompressed;
			ParseBlockHeader(inputBytes, out blockLength, out isCompressed);
			if(!isCompressed)
			{
				var result = new byte[inputBytes.Length - 3];
				Buffer.BlockCopy(inputBytes, 3, result, 0, result.Length);
				return result;
			}
			else
			{
				//Handle other compression types
				return _zlib.Decompress(inputBytes, 3);
			}
		}

		public void ParseBlockHeader(byte[] block, out int blockLength, out bool isCompressed)
		{
			if (block.Length < 3)
				throw new ArgumentOutOfRangeException(nameof(block));

			var rawValue = block[0] | block[1] << 8 | block[2] << 16;
			blockLength = rawValue >> 1;
			isCompressed = (rawValue & 1) == 0;
		}
    }
}
