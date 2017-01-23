using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Protocol
{
	[ProtoContract]
    public class RowIndex
    {
		[ProtoMember(1)]
		public List<RowIndexEntry> Entry { get; } = new List<RowIndexEntry>();
    }
}
