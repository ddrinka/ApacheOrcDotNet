using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Protocol
{
	[ProtoContract]
	public enum ColumnEncodingKind
	{
		[ProtoEnum(Name = @"DIRECT", Value = 0)] Direct = 0,
		[ProtoEnum(Name = @"DICTIONARY", Value = 1)] Dictionary = 1,
		[ProtoEnum(Name = @"DIRECT_V2", Value = 2)] DirectV2 = 2,
		[ProtoEnum(Name = @"DICTIONARY_V2", Value = 3)] DictionaryV2 = 3
	}


	[ProtoContract]
    public class ColumnEncoding
    {
		[ProtoMember(1)] public ColumnEncodingKind Kind { get; set; }
		[ProtoMember(2)] public uint DictionarySize { get; set; }
    }
}
