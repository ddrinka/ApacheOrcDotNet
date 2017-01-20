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
		[ProtoMember(1)] public long Mimumum { get; set; }
		[ProtoMember(2)] public long Maximum { get; set; }
		[ProtoMember(3)] public long? Sum { get; set; }
	}
}
