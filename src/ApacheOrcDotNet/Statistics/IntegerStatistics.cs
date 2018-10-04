using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Statistics
{
	[ProtoContract]
	public class IntegerStatistics : IIntegerStatistics
	{
		[ProtoMember(1, DataFormat = DataFormat.ZigZag)] public long? Minimum { get; set; }
		[ProtoMember(2, DataFormat = DataFormat.ZigZag)] public long? Maximum { get; set; }
		[ProtoMember(3, DataFormat = DataFormat.ZigZag)] public long? Sum { get; set; }
	}
}
