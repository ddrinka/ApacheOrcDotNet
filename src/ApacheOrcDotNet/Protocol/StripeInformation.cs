using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Protocol
{
	[ProtoContract]
    public class StripeInformation
    {
		[ProtoMember(1)] public ulong Offset { get; set; }
		[ProtoMember(2)] public ulong IndexLength { get; set; }
		[ProtoMember(3)] public ulong DataLength { get; set; }
		[ProtoMember(4)] public ulong FooterLength { get; set; }
		[ProtoMember(5)] public ulong NumberOfRows { get; set; }
    }
}
