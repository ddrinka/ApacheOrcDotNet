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
                throw new InvalidOperationException($"Invalid lookup parameters for column '{column.Name}'.");

            return stats.InRange(column.Type, min, max);
        }

        public static bool InRange(this ColumnStatistics stats, ColumnTypeKind columnType, string min, string max)
        {
            switch (columnType)
            {
                case ColumnTypeKind.Boolean:
                    return stats.InRangeBoolean(min == "true", max == "true");
                case ColumnTypeKind.Byte:
                case ColumnTypeKind.Short:
                case ColumnTypeKind.Int:
                case ColumnTypeKind.Long:
                    {
                        var minVal = long.Parse(min);
                        var maxVal = long.Parse(max);
                        return stats.InRangeNumeric(minVal, maxVal);
                    }
                case ColumnTypeKind.Float:
                case ColumnTypeKind.Double:
                    {
                        var minVal = double.Parse(min);
                        var maxVal = double.Parse(max);
                        return stats.InRangeDouble(minVal, maxVal);
                    }
                case ColumnTypeKind.String:
                case ColumnTypeKind.Varchar:
                case ColumnTypeKind.Char:
                    return stats.InRangeString(min, max);
                case ColumnTypeKind.Decimal:
                    {
                        var minVal = decimal.Parse(min);
                        var maxVal = decimal.Parse(max);
                        //TODO it would be better to do a numeric string comparison in the future
                        if (!decimal.TryParse(stats.DecimalStatistics.Minimum, NumberStyles.Number, CultureInfo.InvariantCulture, out var statsMinVal))
                            throw new ArgumentOutOfRangeException($"Unable to parse: '{stats.DecimalStatistics.Minimum}'");
                        if (!decimal.TryParse(stats.DecimalStatistics.Maximum, NumberStyles.Number, CultureInfo.InvariantCulture, out var statsMaxVal))
                            throw new ArgumentOutOfRangeException($"Unable to parse: '{stats.DecimalStatistics.Maximum}'");
                        return stats.InRangeDecimal(minVal, maxVal);
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
            => min <= stats.IntStatistics.Maximum && max >= stats.IntStatistics.Minimum;

        public static bool InRangeDouble(this ColumnStatistics stats, double min, double max)
            => min <= stats.DoubleStatistics.Maximum && max >= stats.DoubleStatistics.Minimum;

        public static bool InRangeString(this ColumnStatistics stats, string min, string max)
            => min.CompareTo(stats.StringStatistics.Maximum) <= 0 && max.CompareTo(stats.StringStatistics.Minimum) >= 0;

        public static bool InRangeDecimal(this ColumnStatistics stats, decimal min, decimal max)
        {
            if (!decimal.TryParse(stats.DecimalStatistics.Minimum, NumberStyles.Number, CultureInfo.InvariantCulture, out var statsMin))
                throw new ArgumentOutOfRangeException($"Unable to parse: '{stats.DecimalStatistics.Minimum}'");
            if (!decimal.TryParse(stats.DecimalStatistics.Maximum, NumberStyles.Number, CultureInfo.InvariantCulture, out var statsMax))
                throw new ArgumentOutOfRangeException($"Unable to parse: '{stats.DecimalStatistics.Maximum}'");

            return min <= statsMax && max >= statsMin;
        }
    }
}
