using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Statistics
{
	[ProtoContract]
    public class TimestampStatistics : IDateTimeStatistics
    {
		DateTime Epoch { get; set; } = new DateTime(2015, 1, 1, 0, 0, 0, DateTimeKind.Utc);

		[ProtoMember(1, DataFormat = DataFormat.ZigZag)] public long? Minimum { get; set; }
		[ProtoMember(2, DataFormat = DataFormat.ZigZag)] public long? Maximum { get; set; }
		[ProtoMember(3, DataFormat = DataFormat.ZigZag)] public long? MinimumUtc { get; set; }
		[ProtoMember(4, DataFormat = DataFormat.ZigZag)] public long? MaximumUtc { get; set; }

		DateTime? IDateTimeStatistics.Minimum
		{
			get
			{
				if (Minimum == null)
					return null;
				return Epoch.AddTicks(Minimum.Value * TimeSpan.TicksPerMillisecond);
			}
		}

		DateTime? IDateTimeStatistics.Maximum
		{
			get
			{
				if (Maximum == null)
					return null;
				return Epoch.AddTicks(Maximum.Value * TimeSpan.TicksPerMillisecond);
			}
		}
	}
}
