using ApacheOrcDotNet.Protocol;
using ApacheOrcDotNet.Statistics;
using System;

namespace ApacheOrcDotNet.OptimizedReader
{
    public static class StatsExtensions
    {
        public static int GetNumValuesInPositionListForStream(this StreamDetail streamDetail, bool compressionEnabled)
        {
            if (!streamDetail.StreamHasAnyPositions())
                return 0;

            int count = 2;  //All streams have a chunk offset and a value offset
            if (compressionEnabled)
                count++;    //If compression is enabled, an offset into the decompressed chunk is also included
            if (streamDetail.StreamHasSecondValuePosition())
                count++;    //Some streams include an additional offset

            return count;
        }

        static bool StreamHasAnyPositions(this StreamDetail streamDetail) =>
            (streamDetail.StreamKind, streamDetail.ColumnType, streamDetail.EncodingKind) switch
            {
                (StreamKind.Present, _, _) => true,
                (StreamKind.Data, _, _) => true,
                (StreamKind.Secondary, _, _) => true,
                //TODO this will need some work to fill in completely
                _ => false,
            };


        static bool StreamHasSecondValuePosition(this StreamDetail streamDetail) =>
            (streamDetail.StreamKind, streamDetail.ColumnType, streamDetail.EncodingKind) switch
            {
                (StreamKind.Present, _, _) => true,
                (StreamKind.Data, ColumnTypeKind.Int, _) => false,
                (StreamKind.Data, ColumnTypeKind.Long, _) => false,
                (StreamKind.Data, ColumnTypeKind.Decimal, _) => false,
                (StreamKind.Data, ColumnTypeKind.String, _) => false,
                (StreamKind.Length, _, ColumnEncodingKind.DictionaryV2) => false,
                (StreamKind.Secondary, _, _) => false, // Already secondary.
                //TODO This will need some work to fill in completely
                _ => throw new NotSupportedException()
            };

        public static Position GetStreamPositionFromStreamType(this StreamDetail stream, bool compressionEnabled, ReadOnlySpan<ulong> positions)
        {
            int positionIndex = 0;
            var chunkFileOffset = stream.FileOffset + (long)positions[positionIndex++];
            var decompressedOffset = compressionEnabled ? (int)positions[positionIndex++] : 0;
            var valueOffset = (int)positions[positionIndex++];
            var valueOffset2 = stream.StreamHasSecondValuePosition() ? (int)positions[positionIndex++] : 0;

            return new Position(chunkFileOffset, decompressedOffset, valueOffset, valueOffset2);
        }

        public static bool InRange(this ColumnStatistics stats, ColumnTypeKind columnType, string min, string max)
        {
            switch (columnType)
            {
                case ColumnTypeKind.Boolean:
                    return InRangeBoolean(stats, min == "true", max == "true");
                case ColumnTypeKind.Byte:
                case ColumnTypeKind.Short:
                case ColumnTypeKind.Int:
                case ColumnTypeKind.Long:
                    {
                        var minVal = long.Parse(min);
                        var maxVal = long.Parse(max);
                        return InRangeNumeric(stats, minVal, maxVal);
                    }
                case ColumnTypeKind.Float:
                case ColumnTypeKind.Double:
                    {
                        var minVal = double.Parse(min);
                        var maxVal = double.Parse(max);
                        return InRangeDouble(stats, minVal, maxVal);
                    }
                case ColumnTypeKind.String:
                case ColumnTypeKind.Varchar:
                case ColumnTypeKind.Char:
                    return InRangeString(stats, min, max);
                case ColumnTypeKind.Decimal:
                    {
                        var minVal = decimal.Parse(min);
                        var maxVal = decimal.Parse(max);
                        //TODO it would be better to do a numeric string comparison in the future
                        if (!decimal.TryParse(stats.DecimalStatistics.Minimum, out var statsMinVal))
                            throw new ArgumentOutOfRangeException($"Unable to parse: '{stats.DecimalStatistics.Minimum}'");
                        if (!decimal.TryParse(stats.DecimalStatistics.Maximum, out var statsMaxVal))
                            throw new ArgumentOutOfRangeException($"Unable to parse: '{stats.DecimalStatistics.Maximum}'");
                        return InRangeDecimal(stats, minVal, maxVal);
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
            if (!decimal.TryParse(stats.DecimalStatistics.Minimum, out var statsMin))
                throw new ArgumentOutOfRangeException($"Unable to parse: '{stats.DecimalStatistics.Minimum}'");
            if (!decimal.TryParse(stats.DecimalStatistics.Maximum, out var statsMax))
                throw new ArgumentOutOfRangeException($"Unable to parse: '{stats.DecimalStatistics.Maximum}'");

            return min <= statsMax && max >= statsMin;
        }
    }
}
