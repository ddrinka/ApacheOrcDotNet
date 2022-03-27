using ApacheOrcDotNet.Protocol;
using ApacheOrcDotNet.Statistics;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader
{
    public static class StatsExtensions
    {
        public static int GetNumValuesInPositionListForStream(this StreamDetail streamDetail, bool compressionEnabled) =>
            (streamDetail.StreamKind, streamDetail.ColumnType, streamDetail.EncodingKind, compressionEnabled) switch
            {
                (StreamKind.Present, _, _, true) => 4,
                (StreamKind.Present, _, _, false) => 3,
                (StreamKind.Data, ColumnTypeKind.Int, _, true) => 3,
                (StreamKind.Data, ColumnTypeKind.Int, _, false) => 2,
                (StreamKind.Length, _, _, true) => throw new NotImplementedException(),
                (StreamKind.Length, _, _, false) => throw new NotImplementedException(),
                (StreamKind.Secondary, _, _, true) => throw new NotImplementedException(),
                (StreamKind.Secondary, _, _, false) => throw new NotImplementedException(),
                //TODO This will need some work to fill in completely
                _ => throw new ArgumentException()
            };

        public static bool InRange(this ColumnStatistics stats, ColumnTypeKind columnType, string min, string max)
        {
            switch (columnType)
            {
                case ColumnTypeKind.Boolean:
                    {
                        if (min != max)
                            return true;
                        if (min == "false" && stats.BooleanStatistics.FalseCount > 0)
                            return true;
                        if (min == "true" && stats.BooleanStatistics.TrueCount > 0)
                            return true;
                        return false;
                    }
                case ColumnTypeKind.Byte:
                case ColumnTypeKind.Short:
                case ColumnTypeKind.Int:
                case ColumnTypeKind.Long:
                    {
                        var minVal = long.Parse(min);
                        var maxVal = long.Parse(max);
                        return minVal <= stats.IntStatistics.Maximum && maxVal >= stats.IntStatistics.Minimum;
                    }
                case ColumnTypeKind.Float:
                case ColumnTypeKind.Double:
                    {
                        var minVal = double.Parse(min);
                        var maxVal = double.Parse(max);
                        return minVal <= stats.DoubleStatistics.Maximum && maxVal >= stats.DoubleStatistics.Minimum;
                    }
                case ColumnTypeKind.String:
                case ColumnTypeKind.Varchar:
                case ColumnTypeKind.Char:
                    {
                        return min.CompareTo(stats.StringStatistics.Maximum) <= 0 && max.CompareTo(stats.StringStatistics.Minimum) >= 0;
                    }
                case ColumnTypeKind.Decimal:
                    {
                        var minVal = decimal.Parse(min);
                        var maxVal = decimal.Parse(max);
                        //TODO it would be better to do a numeric string comparison in the future
                        if (!decimal.TryParse(stats.DecimalStatistics.Minimum, out var statsMinVal))
                            throw new ArgumentOutOfRangeException($"Unable to parse: '{stats.DecimalStatistics.Minimum}'");
                        if (!decimal.TryParse(stats.DecimalStatistics.Maximum, out var statsMaxVal))
                            throw new ArgumentOutOfRangeException($"Unable to parse: '{stats.DecimalStatistics.Maximum}'");
                        return minVal <= statsMinVal && maxVal >= statsMaxVal;
                    }
                default:
                    throw new NotImplementedException($"Range check for {columnType} not implemented");
            }
        }
    }
}
