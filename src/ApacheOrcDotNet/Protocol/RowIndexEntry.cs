using ApacheOrcDotNet.Statistics;
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Protocol
{
	[ProtoContract]
    public class RowIndexEntry
    {
		[ProtoMember(1, IsPacked = true)]
		public List<ulong> Positions { get; } = new List<ulong>();
		[ProtoMember(2)]
		public ColumnStatistics Statistics { get; } = new ColumnStatistics();
    }
}
