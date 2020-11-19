using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Protocol
{
	[ProtoContract]
	public enum ColumnTypeKind
	{
		Boolean = 0,
		Byte = 1,
		Short = 2,
		Int = 3,
		Long = 4,
		Float = 5,
		Double = 6,
		String = 7,
		Binary = 8,
		Timestamp = 9,
		List = 10,
		Map = 11,
		Struct = 12,
		Union = 13,
		Decimal = 14,
		Date = 15,
		Varchar = 16,
		Char = 17
	}

	[ProtoContract]
    public class ColumnType
    {
		[ProtoMember(1)] public ColumnTypeKind Kind { get; set; }
		[ProtoMember(2, IsPacked = true)]
		public List<uint> SubTypes { get; } = new List<uint>();
		[ProtoMember(3)]
		public List<string> FieldNames { get; } = new List<string>();
		[ProtoMember(4)] public uint MaximumLength { get; set; }
		[ProtoMember(5)] public uint Precision { get; set; }
		[ProtoMember(6)] public uint Scale { get; set; }
    }
}
