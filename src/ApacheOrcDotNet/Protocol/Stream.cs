using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Protocol
{
	[ProtoContract]
	public enum StreamKind
	{
		Present = 0,
		Data = 1,
		Length = 2,
		DictionaryData = 3,
		DictionaryCount = 4,
		Secondary = 5,
		RowIndex = 6,
		BloomFilter = 7,
		BloomFilterUtf8 = 8
	}

	[ProtoContract]
    public class Stream
    {
		[ProtoMember(1)] public StreamKind Kind { get; set; }
		[ProtoMember(2)] public uint Column { get; set; }
		[ProtoMember(3)] public ulong Length { get; set; }
    }
}
