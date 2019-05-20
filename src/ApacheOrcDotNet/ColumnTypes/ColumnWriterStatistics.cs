using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
	public class ColumnWriterStatistics
	{
		public List<long> CompressedBufferOffsets { get; } = new List<long>();
		public List<long> DecompressedOffsets { get; } = new List<long>();
		public List<long> RleValuesToConsume { get; } = new List<long>();

		public void AnnotatePosition(long compressedBufferOffset, long decompressedOffset, long rleValuesToConsume)
		{
			CompressedBufferOffsets.Add(compressedBufferOffset);
			DecompressedOffsets.Add(decompressedOffset);
			RleValuesToConsume.Add(rleValuesToConsume);
		}

		public void AnnotatePosition(long uncompressedOffset, long rleValuesToConsume)
		{
			CompressedBufferOffsets.Add(uncompressedOffset);
			RleValuesToConsume.Add(rleValuesToConsume);
		}

		public void FillPositionList(List<ulong> positions, Func<int,bool> bufferIndexMustBeIncluded)
		{
			//If we weren't dealing with compressed data, only two values are written rather than three
			bool haveSecondValues = DecompressedOffsets.Count != 0;
			for (int i = 0; i < CompressedBufferOffsets.Count; i++)
			{
                if (!bufferIndexMustBeIncluded(i))
                    continue;

				positions.Add((ulong)CompressedBufferOffsets[i]);
				if (haveSecondValues)
					positions.Add((ulong)DecompressedOffsets[i]);
				positions.Add((ulong)RleValuesToConsume[i]);
			}
		}
	}
}
