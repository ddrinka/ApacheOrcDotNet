using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Protocol
{
	[ProtoContract]
    public class UserMetadataItem
    {
		[ProtoMember(1)] public string Name { get; set; }
		[ProtoMember(2)] public byte[] Value { get; set; }
    }
}
