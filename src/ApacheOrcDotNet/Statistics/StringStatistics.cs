using ProtoBuf;

namespace ApacheOrcDotNet.Statistics {
    [ProtoContract]
    public class StringStatistics : IStringStatistics
    {
		[ProtoMember(1)] public string Minimum { get; set; }
		[ProtoMember(2)] public string Maximum { get; set; }
		[ProtoMember(3, DataFormat = DataFormat.ZigZag)] public long? Sum { get; set; }
    }
}
