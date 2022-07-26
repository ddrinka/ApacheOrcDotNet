using System;
using System.Globalization;

namespace ApacheOrcDotNet.OptimizedReader
{
    public class FilterValues
    {
        public static CultureInfo _invariantCulture = CultureInfo.InvariantCulture;

        private FilterValues(string min, string max)
        {
            Min = min;
            Max = max;
        }

        public string Min { get; }
        public string Max { get; }

        public static FilterValues CreateFromInteger(long min, long max)
            => new FilterValues(min.ToString(_invariantCulture), max.ToString(_invariantCulture));

        public static FilterValues CreateFromDecimal(decimal min, decimal max)
            => new FilterValues(min.ToString(_invariantCulture), max.ToString(_invariantCulture));

        public static FilterValues CreateFromString(string min, string max)
            => new FilterValues(min, max);

        public static FilterValues CreateFromDate(DateTime min, DateTime max)
        {
            var minVal = (long)(min - Constants.UnixEpochUnspecified).TotalDays;
            var maxVal = (long)(max - Constants.UnixEpochUnspecified).TotalDays;

            return CreateFromInteger(minVal, maxVal);
        }

        public static FilterValues CreateFromTimestamp(DateTime min, DateTime max)
        {
            var minVal = (min - Constants.UnixEpochUtc).Ticks / TimeSpan.TicksPerMillisecond;
            var maxVal = (max - Constants.UnixEpochUtc).Ticks / TimeSpan.TicksPerMillisecond;

            return CreateFromInteger(minVal, maxVal);
        }

        public static FilterValues CreateFromTime(TimeSpan min, TimeSpan max)
            => CreateFromDecimal((decimal)min.TotalSeconds, (decimal)max.TotalSeconds);
    }
}
