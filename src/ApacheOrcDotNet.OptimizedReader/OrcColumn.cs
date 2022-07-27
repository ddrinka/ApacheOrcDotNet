using ApacheOrcDotNet.Protocol;
using System;
using System.Globalization;

namespace ApacheOrcDotNet.OptimizedReader
{
    public class OrcColumn : IEquatable<OrcColumn>
    {
        private static CultureInfo _invariantCulture = CultureInfo.InvariantCulture;

        public OrcColumn(int id, string name, ColumnTypeKind type)
        {
            Id = id;
            Name = name;
            Type = type;
        }

        public int Id { get; }
        public string Name { get; }
        public ColumnTypeKind Type { get; }

        public string Min { get; private set; }
        public string Max { get; private set; }

        public override bool Equals(object obj)
        {
            if (obj is OrcColumn other)
                return Equals(other);

            return false;
        }

        public bool Equals(OrcColumn other)
            => Id == other.Id && Name == other.Name && Type == other.Type;

        public override int GetHashCode() => HashCode.Combine(Id, Name, Type);

        public void SetIntegerFilter(long min, long max)
        {
            Min = min.ToString(_invariantCulture);
            Max = max.ToString(_invariantCulture);
        }

        public void SetDecimalFilter(decimal min, decimal max)
        {
            Min = min.ToString(_invariantCulture);
            Max = max.ToString(_invariantCulture);
        }

        public void SetStringFilter(string min, string max)
        {
            Min = min;
            Max = max;
        }

        public void SetDateFilter(DateTime min, DateTime max)
        {
            var minVal = (long)(min - Constants.UnixEpochUnspecified).TotalDays;
            var maxVal = (long)(max - Constants.UnixEpochUnspecified).TotalDays;

            SetIntegerFilter(minVal, maxVal);
        }

        public void SetTimestampFilter(DateTime min, DateTime max)
        {
            var minVal = (min - Constants.UnixEpochUtc).Ticks / TimeSpan.TicksPerMillisecond;
            var maxVal = (max - Constants.UnixEpochUtc).Ticks / TimeSpan.TicksPerMillisecond;

            SetIntegerFilter(minVal, maxVal);
        }

        public void SetTimeFilter(TimeSpan min, TimeSpan max)
        {
            var minVal = (decimal)min.TotalSeconds;
            var maxVal = (decimal)max.TotalSeconds;

            SetDecimalFilter(minVal, maxVal);
        }
    }
}
