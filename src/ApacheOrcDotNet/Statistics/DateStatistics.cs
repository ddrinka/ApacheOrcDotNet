using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Statistics
{
	[ProtoContract]
    public class DateStatistics : IDateTimeStatistics
    {
		DateTime Epoch { get; set; } = new DateTime(2015, 1, 1, 0, 0, 0, DateTimeKind.Utc);

		[ProtoMember(1, DataFormat = DataFormat.ZigZag)] public int? Minimum { get; set; }
		[ProtoMember(2, DataFormat = DataFormat.ZigZag)] public int? Maximum { get; set; }

		DateTime? IDateTimeStatistics.Minimum
		{
			get
			{
				if (!Minimum.HasValue)
					return null;
				return Epoch.AddTicks(Minimum.Value * TimeSpan.TicksPerDay);
			}
		}

		DateTime? IDateTimeStatistics.Maximum
		{
			get
			{
				if (!Maximum.HasValue)
					return null;
				return Epoch.AddTicks(Maximum.Value * TimeSpan.TicksPerDay);
			}
		}
	}
}
