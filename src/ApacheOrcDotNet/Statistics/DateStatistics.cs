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

		[ProtoMember(1)] public int Minimum { get; set; }
		[ProtoMember(2)] public int Maximum { get; set; }

		DateTime IDateTimeStatistics.Minimum
		{
			get
			{
				return Epoch.AddTicks(Minimum * TimeSpan.TicksPerDay);
			}
		}

		DateTime IDateTimeStatistics.Maximum
		{
			get
			{
				return Epoch.AddTicks(Maximum * TimeSpan.TicksPerDay);
			}
		}
	}
}
