using ProtoBuf;

namespace ApacheOrcDotNet.Statistics {
    [ProtoContract]
    public class BinaryStatistics : IBinaryStatistics {
        [ProtoMember(1, DataFormat = DataFormat.ZigZag)] public long? Sum { get; set; }
    }
}
