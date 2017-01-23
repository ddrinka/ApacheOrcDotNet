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
		[ProtoEnum(Name = @"BOOLEAN", Value = 0)] Boolean = 0,
		[ProtoEnum(Name = @"BYTE", Value = 1)] Byte = 1,
		[ProtoEnum(Name = @"SHORT", Value = 2)] Short = 2,
		[ProtoEnum(Name = @"INT", Value = 3)] Int = 3,
		[ProtoEnum(Name = @"LONG", Value = 4)] Long = 4,
		[ProtoEnum(Name = @"FLOAT", Value = 5)] Float = 5,
		[ProtoEnum(Name = @"DOUBLE", Value = 6)] Double = 6,
		[ProtoEnum(Name = @"STRING", Value = 7)] String = 7,
		[ProtoEnum(Name = @"BINARY", Value = 8)] Binary = 8,
		[ProtoEnum(Name = @"TIMESTAMP", Value = 9)] Timestamp = 9,
		[ProtoEnum(Name = @"LIST", Value = 10)] List = 10,
		[ProtoEnum(Name = @"MAP", Value = 11)] Map = 11,
		[ProtoEnum(Name = @"STRUCT", Value = 12)] Struct = 12,
		[ProtoEnum(Name = @"UNION", Value = 13)] Union = 13,
		[ProtoEnum(Name = @"DECIMAL", Value = 14)] Decimal = 14,
		[ProtoEnum(Name = @"DATE", Value = 15)] Date = 15,
		[ProtoEnum(Name = @"VARCHAR", Value = 16)] Varchar = 16,
		[ProtoEnum(Name = @"CHAR", Value = 17)] Char = 17
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
