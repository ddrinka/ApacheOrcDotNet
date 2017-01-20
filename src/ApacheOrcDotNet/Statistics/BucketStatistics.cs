using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Statistics
{
	[ProtoContract]
	public class BucketStatistics : IBucketStatistics
	{
		[ProtoMember(1, IsPacked = true)]
		public List<ulong> Count { get; set; } = new List<ulong>();
	}
}
