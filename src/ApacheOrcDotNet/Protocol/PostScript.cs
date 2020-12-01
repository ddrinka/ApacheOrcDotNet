using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Protocol
{
	public enum CompressionKind
	{
		None = 0,
		Zlib = 1,
		Snappy = 2,
		Lzo = 3,
		Lz4 = 4,
		Zstd = 5
	}

	[ProtoContract]
    public class PostScript
    {
		[ProtoMember(1)] public ulong FooterLength { get; set; }
		[ProtoMember(2)] public CompressionKind Compression { get; set; }
		[ProtoMember(3)] public ulong CompressionBlockSize { get; set; }
		[ProtoMember(4, IsPacked = true)]
		public List<uint> Version { get; set; } = new List<uint>();
		public uint? VersionMajor => Version.Count > 0 ? (uint?)Version[0] : null;
		public uint? VersionMinor => Version.Count > 1 ? (uint?)Version[1] : null;
		[ProtoMember(5)] public ulong MetadataLength { get; set; }
		[ProtoMember(6)] public uint WriterVersion { get; set; }


		[ProtoMember(8000)] public string Magic { get; set; }
    }
}
