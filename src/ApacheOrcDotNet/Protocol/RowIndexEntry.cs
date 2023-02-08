using ApacheOrcDotNet.Statistics;
using ProtoBuf;
using System.Collections.Generic;

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
