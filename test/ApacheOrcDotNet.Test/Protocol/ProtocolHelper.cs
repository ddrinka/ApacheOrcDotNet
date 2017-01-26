using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.Infrastructure;
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

		public ProtocolHelper(string dataFileName)
		{
			_dataFileHelper = new DataFileHelper(dataFileName);
		}

		public int GetPostscriptLength()
		{
			var lastByte = _dataFileHelper.Read(_dataFileHelper.Length - 1, 1)[0];
			return lastByte;
		}

		public Stream GetPostscriptStream(int postscriptLength)
		{
			var offset = _dataFileHelper.Length - 1 - postscriptLength;
			return _dataFileHelper.GetStreamSegment(offset, (ulong)postscriptLength);
		}

		public Stream GetFooterCompressedStream(int postscriptLength, ulong footerLength)
		{
			var offset = _dataFileHelper.Length - 1 - postscriptLength - (long)footerLength;
			return _dataFileHelper.GetStreamSegment(offset, footerLength);
		}

		public Stream GetMetadataCompressedStream(int postscriptLength, ulong footerLength, ulong metadataLength)
		{
			var offset = _dataFileHelper.Length - 1 - postscriptLength - (long)footerLength - (long)metadataLength;
			return _dataFileHelper.GetStreamSegment(offset, metadataLength);
		}

		public Stream GetStripeFooterCompressedStream(ulong stripeOffset, ulong indexLength, ulong dataLength, ulong footerLength)
		{
			var offset = stripeOffset + indexLength + dataLength;
			return _dataFileHelper.GetStreamSegment((long)offset, footerLength);
		}

		public Stream GetRowIndexCompressedStream(ulong offset, ulong length)
		{
			return _dataFileHelper.GetStreamSegment((long)offset, length);
		}

		public Stream GetDataCompressedStream(ulong offset, ulong length)
		{
			return _dataFileHelper.GetStreamSegment((long)offset, length);
		}

		/// <summary>
		/// Provides a Stream that when read from, reads consecutive blocks of compressed data from an ORC Stream.
		/// All data in the <paramref name="inputStream"/> will be consumed.
		/// </summary>
		public Stream GetDecompressingStream(Stream inputStream)
		{
			return new ConcatenatingStream(() =>
			{
				int blockLength;
				bool isCompressed;
				bool headerAvailable = ReadBlockHeader(inputStream, out blockLength, out isCompressed);
				if (!headerAvailable)
					return null;

				var streamSegment = new StreamSegment(inputStream, blockLength, true);

				if (!isCompressed)
					return streamSegment;
				else
				{
					//Handle other compression types
					return new ZLibStream(streamSegment);
				}
			}, false);
		}

		bool ReadBlockHeader(Stream inputStream, out int blockLength, out bool isCompressed)
		{
			var firstByte = inputStream.ReadByte();
			if (firstByte < 0)		//End of stream
			{
				blockLength = 0;
				isCompressed = false;
				return false;
			}

			//From here it's a data error if the stream ends
			var rawValue = firstByte | inputStream.CheckedReadByte() << 8 | inputStream.CheckedReadByte() << 16;
			blockLength = rawValue >> 1;
			isCompressed = (rawValue & 1) == 0;

			return true;
		}
    }
}
