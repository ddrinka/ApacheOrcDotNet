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
                (StreamKind.Data, ColumnTypeKind.String, _) => false,
                (StreamKind.Length, _, ColumnEncodingKind.DictionaryV2) => false,
                (StreamKind.Secondary, _, _) => throw new NotImplementedException(),
                //TODO This will need some work to fill in completely
                _ => throw new NotSupportedException()
            };

        public static StreamPosition GetStreamPositionFromStreamType(this StreamDetail stream, bool compressionEnabled, ReadOnlySpan<ulong> positions)
        {
            int positionIndex = 0;
            var chunkFileOffset = stream.FileOffset + (long)positions[positionIndex++];
            var decompressedOffset = compressionEnabled ? (int?)positions[positionIndex++] : null;
            var valueOffset = (int)positions[positionIndex++];
            var valueOffset2 = stream.StreamHasSecondValuePosition() ? (int?)positions[positionIndex++] : null;

            return new StreamPosition(chunkFileOffset, decompressedOffset, valueOffset, valueOffset2);
        }

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
