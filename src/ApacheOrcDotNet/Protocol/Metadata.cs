using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Protocol
{
	[ProtoContract]
	public class Metadata
	{
		[ProtoMember(1)]
		public List<StripeStatistics> StripeStats { get; set; } = new List<StripeStatistics>();
    }
}
