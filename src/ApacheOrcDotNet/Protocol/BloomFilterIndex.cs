using ProtoBuf;
using System.Collections.Generic;

namespace ApacheOrcDotNet.Protocol {
    [ProtoContract]
    public class BloomFilterIndex
    {
		[ProtoMember(1)]
		public List<BloomFilter> BloomFilter { get; } = new List<Protocol.BloomFilter>();
    }
}
