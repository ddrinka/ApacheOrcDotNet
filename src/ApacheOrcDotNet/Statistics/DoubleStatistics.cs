﻿using ProtoBuf;

namespace ApacheOrcDotNet.Statistics {
    [ProtoContract]
    public class DoubleStatistics : IDoubleStatistics
    {
		[ProtoMember(1)] public double? Minimum { get; set; }
		[ProtoMember(2)] public double? Maximum { get; set; }
		[ProtoMember(3)] public double? Sum { get; set; }
    }
}
