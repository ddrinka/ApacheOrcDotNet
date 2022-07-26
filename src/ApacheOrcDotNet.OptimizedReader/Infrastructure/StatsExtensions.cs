using ApacheOrcDotNet.Protocol;
using ApacheOrcDotNet.Statistics;
using System;
using System.Globalization;

namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public static class StatsExtensions
    {
        public static bool InRange(this ColumnStatistics stats, OrcColumn column, string min, string max)
        {
            if (string.IsNullOrEmpty(min) && string.IsNullOrEmpty(max))
                throw new ArgumentException($"Lookup parameters for column '{column.Name}' cannot be null.");

            return stats.InRange(column.Type, min, max);
        }

        public static bool InRange(this ColumnStatistics stats, ColumnTypeKind columnType, string min, string max)
        {
            if (min == null || max == null)
                throw new ArgumentException($"Lookup parameters cannot be null.");

            switch (columnType)
            {
                case ColumnTypeKind.Boolean:
                    return stats.InRangeBoolean(min == "true", max == "true");
                case ColumnTypeKind.Byte:
                case ColumnTypeKind.Short:
                case ColumnTypeKind.Int:
                case ColumnTypeKind.Long:
                    {
                        var minVal = long.Parse(min, CultureInfo.InvariantCulture);
                        var maxVal = long.Parse(max, CultureInfo.InvariantCulture);
                        return stats.InRangeNumeric(minVal, maxVal);
                    }
                case ColumnTypeKind.Float:
                case ColumnTypeKind.Double:
                    {
                        var minVal = double.Parse(min, CultureInfo.InvariantCulture);
                        var maxVal = double.Parse(max, CultureInfo.InvariantCulture);
                        return stats.InRangeDouble(minVal, maxVal);
                    }
                case ColumnTypeKind.String:
                case ColumnTypeKind.Varchar:
                case ColumnTypeKind.Char:
                    return stats.InRangeString(min, max);
                case ColumnTypeKind.Decimal:
                    {
                        var minVal = decimal.Parse(min, NumberStyles.Number, CultureInfo.InvariantCulture);
                        var maxVal = decimal.Parse(max, NumberStyles.Number, CultureInfo.InvariantCulture);
                        return stats.InRangeDecimal(minVal, maxVal);
                    }
                case ColumnTypeKind.Date:
                    {
                        var minVal = int.Parse(min, CultureInfo.InvariantCulture);
                        var maxVal = int.Parse(max, CultureInfo.InvariantCulture);
                        return stats.InRangeDate(minVal, maxVal);
                    }
                case ColumnTypeKind.Timestamp:
                    {
                        var minVal = long.Parse(min, CultureInfo.InvariantCulture);
                        var maxVal = long.Parse(max, CultureInfo.InvariantCulture);
                        return stats.InRangeTimespan(minVal, maxVal);
                    }
                default:
                    throw new NotImplementedException($"Range check for {columnType} not implemented");
            }
        }

        public static bool InRangeBoolean(this ColumnStatistics stats, bool min, bool max)
        {
            if (min != max)
                return true;
            if (!min && stats.BooleanStatistics.FalseCount > 0)
                return true;
            if (min && stats.BooleanStatistics.TrueCount > 0)
                return true;
            return false;
        }

        public static bool InRangeNumeric(this ColumnStatistics stats, long min, long max)
        { 
            return min <= stats.IntStatistics.Maximum && max >= stats.IntStatistics.Minimum;
        }

        public static bool InRangeDouble(this ColumnStatistics stats, double min, double max)
        { 
            return min <= stats.DoubleStatistics.Maximum && max >= stats.DoubleStatistics.Minimum;
        }

        public static bool InRangeString(this ColumnStatistics stats, string min, string max)
        {
            return min.CompareTo(stats.StringStatistics.Maximum) <= 0 && max.CompareTo(stats.StringStatistics.Minimum) >= 0;
        }

        public static bool InRangeDecimal(this ColumnStatistics stats, decimal min, decimal max)
        { 
            return min <= decimal.Parse(stats.DecimalStatistics.Maximum, NumberStyles.Number, CultureInfo.InvariantCulture)
                && max >= decimal.Parse(stats.DecimalStatistics.Minimum, NumberStyles.Number, CultureInfo.InvariantCulture);
        }

        public static bool InRangeDate(this ColumnStatistics stats, int min, int max)
        {
            return min <= stats.DateStatistics.Maximum && max >= stats.DateStatistics.Minimum;
        }

        public static bool InRangeTimespan(this ColumnStatistics stats, long min, long max)
        { 
            return min <= stats.TimestampStatistics.MaximumUtc && max >= stats.TimestampStatistics.MinimumUtc;
        }
    }
}
