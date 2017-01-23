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
		[ProtoEnum(Name = @"PRESENT", Value = 0)] Present = 0,
		[ProtoEnum(Name = @"DATA", Value = 1)] Data = 1,
		[ProtoEnum(Name = @"LENGTH", Value = 2)] Length = 2,
		[ProtoEnum(Name = @"DICTIONARY_DATA", Value = 3)] DictionaryData = 3,
		[ProtoEnum(Name = @"DICTIONARY_COUNT", Value = 4)] DictionaryCount = 4,
		[ProtoEnum(Name = @"SECONDARY", Value = 5)] Secondary = 5,
		[ProtoEnum(Name = @"ROW_INDEX", Value = 6)] RowIndex = 6,
		[ProtoEnum(Name = @"BLOOM_FILTER", Value = 7)] BloomFilter = 7,
		[ProtoEnum(Name = @"BLOOM_FILTER_UTF8", Value = 8)] BloomFilterUtf8 = 8
	}

	[ProtoContract]
    public class Stream
    {
		[ProtoMember(1)] public StreamKind Kind { get; set; }
		[ProtoMember(2)] public uint Column { get; set; }
		[ProtoMember(3)] public ulong Length { get; set; }
    }
}
