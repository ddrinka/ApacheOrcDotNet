using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Protocol
{
	[ProtoContract]
    public class StripeFooter
    {
		[ProtoMember(1)]
		public List<Stream> Streams { get; } = new List<Stream>();
		[ProtoMember(2)]
		public List<ColumnEncoding> Columns { get; } = new List<ColumnEncoding>();
		[ProtoMember(3)] public string WriterTimezone { get; set; }
    }
}
