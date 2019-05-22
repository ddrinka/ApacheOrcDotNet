using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
	public class ColumnWriterStatistics
	{
        public List<List<ulong>> PositionTuples = new List<List<ulong>>();

        public void AnnotatePosition(long storedBufferOffset, long? decompressedOffset = null, int? rleValuesToConsume = null, int? bitsToConsume = null)
        {
            var newTuple = new List<ulong> { (ulong)storedBufferOffset };
            if (decompressedOffset.HasValue)
                newTuple.Add((ulong)decompressedOffset.Value);
            if (rleValuesToConsume.HasValue)
                newTuple.Add((ulong)rleValuesToConsume.Value);
            if (bitsToConsume.HasValue)
                newTuple.Add((ulong)bitsToConsume.Value);
            PositionTuples.Add(newTuple);
        }

		public void FillPositionList(List<ulong> positions, Func<int,bool> bufferIndexMustBeIncluded)
		{
            for (int i = 0; i < PositionTuples.Count; i++)
            {
                if (!bufferIndexMustBeIncluded(i))
                    continue;

                positions.AddRange(PositionTuples[i]);
            }
		}
	}
}
