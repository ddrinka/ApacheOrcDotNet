using ApacheOrcDotNet.Statistics;
using ProtoBuf;
using System.Collections.Generic;

namespace ApacheOrcDotNet.Protocol {
    [ProtoContract]
    public class StripeStatistics
    {
		[ProtoMember(1)]
		public List<ColumnStatistics> ColStats { get; } = new List<ColumnStatistics>();
    }
}
