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
		[ProtoMember(1)] public string Minimum { get; set; }
		decimal IDecimalStatistics.Minimum => decimal.Parse(Minimum);
		[ProtoMember(2)] public string Maximum { get; set; }
		decimal IDecimalStatistics.Maximum => decimal.Parse(Maximum);
		[ProtoMember(3)] public string Sum { get; set; }
		decimal IDecimalStatistics.Sum => decimal.Parse(Sum);
	}
}
