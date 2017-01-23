using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Protocol
{
	[ProtoContract]
    public class BloomFilterIndex
    {
		[ProtoMember(1)]
		public List<BloomFilter> BloomFilter { get; } = new List<Protocol.BloomFilter>();
    }
}
