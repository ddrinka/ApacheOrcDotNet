using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Protocol
{
	[ProtoContract]
	public class FileTail
	{
		[ProtoMember(1)] public PostScript PostScript { get; set; }
		[ProtoMember(2)] public Footer Footer { get; set; }
		[ProtoMember(3)] public ulong FileLength { get; set; }
		[ProtoMember(4)] public ulong PostScriptLength { get; set; }
    }
}
