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

		[ProtoMember(1)] public long Minimum { get; set; }
		[ProtoMember(2)] public long Maximum { get; set; }

		DateTime IDateTimeStatistics.Minimum
		{
			get
			{
				return Epoch.AddTicks(Minimum * TimeSpan.TicksPerMillisecond);
			}
		}

		DateTime IDateTimeStatistics.Maximum
		{
			get
			{
				return Epoch.AddTicks(Maximum * TimeSpan.TicksPerMillisecond);
			}
		}
	}
}
