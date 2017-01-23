using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Statistics
{
	[ProtoContract]
    public class ColumnStatistics : IColumnStatistics
    {
		[ProtoMember(1)] public ulong NumberOfValues { get; set; }
		[ProtoMember(2)] public IntegerStatistics IntStatistics { get; set; }
		IIntegerStatistics IColumnStatistics.IntStatistics => IntStatistics;
		[ProtoMember(3)] public DoubleStatistics DoubleStatistics { get; set; }
		IDoubleStatistics IColumnStatistics.DoubleStatistics => DoubleStatistics;
		[ProtoMember(4)] public StringStatistics StringStatistics { get; set; }
		IStringStatistics IColumnStatistics.StringStatistics => StringStatistics;
		[ProtoMember(5)] public BucketStatistics BucketStatistics { get; set; }
		public IBooleanStatistics BooleanStatistics => BucketStatistics;
		[ProtoMember(6)] public DecimalStatistics DecimalStatistics { get; set; }
		IDecimalStatistics IColumnStatistics.DecimalStatistics => DecimalStatistics;
		[ProtoMember(7)] public DateStatistics DateStatistics { get; set; }
		IDateTimeStatistics IColumnStatistics.DateStatistics => DateStatistics;
		[ProtoMember(8)] public BinaryStatistics BinaryStatistics { get; set; }
		IBinaryStatistics IColumnStatistics.BinaryStatistics => BinaryStatistics;
		[ProtoMember(9)] public TimestampStatistics TimestampStatistics { get; set; }
		IDateTimeStatistics IColumnStatistics.TimestampStatistics => TimestampStatistics;
		[ProtoMember(10)] public bool HasNull { get; set; }
	}
}
