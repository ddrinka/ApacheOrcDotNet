using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Statistics
{
	[ProtoContract]
    public class BinaryStatistics : IBinaryStatistics
    {
		[ProtoMember(1)] public long Sum { get; set; }
    }
}
