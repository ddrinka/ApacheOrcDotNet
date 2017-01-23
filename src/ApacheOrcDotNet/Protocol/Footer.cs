using ApacheOrcDotNet.Statistics;
using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Protocol
{
	[ProtoContract]
	public class Footer
	{
		[ProtoMember(1)] public ulong HeaderLength { get; set; }
		[ProtoMember(2)] public ulong ContentLength { get; set; }
		[ProtoMember(3)]
		public List<StripeInformation> Stripes { get; } = new List<StripeInformation>();
		[ProtoMember(4)]
		public List<ColumnType> Types { get; } = new List<ColumnType>();
		[ProtoMember(5)]
		public List<UserMetadataItem> Metadata { get; } = new List<UserMetadataItem>();
		[ProtoMember(6)] public ulong NumberOfRows { get; set; }
		[ProtoMember(7)]
		public List<ColumnStatistics> Statistics { get; } = new List<ColumnStatistics>();
		[ProtoMember(8)] public uint RowIndexStride { get; set; }
    }
}
