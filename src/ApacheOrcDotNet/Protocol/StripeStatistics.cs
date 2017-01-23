using ApacheOrcDotNet.Statistics;
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Protocol
{
	[ProtoContract]
    public class StripeStatistics
    {
		[ProtoMember(1)]
		public List<ColumnStatistics> ColStats { get; } = new List<ColumnStatistics>();
    }
}
