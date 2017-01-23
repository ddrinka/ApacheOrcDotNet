using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Statistics
{
	[ProtoContract]
	public class DecimalStatistics : IDecimalStatistics
	{
		[ProtoMember(1)] public decimal Minimum { get; set; }
		[ProtoMember(2)] public decimal Maximum { get; set; }
		[ProtoMember(3)] public decimal Sum { get; set; }
	}
}
