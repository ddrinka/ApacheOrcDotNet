using System;
using System.Linq;
using Xunit;

namespace ApacheOrcDotNet.OptimizedReader.Test.ColumnTypes.WithoutNulls
{
    public class _Filter_Test : _BaseColumnTypeWithoutNulls
    {
        [Fact]
        public void Filter_Single_RowEntry()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var sourceColumn = reader.GetColumn("source");
            var symbolColumn = reader.GetColumn("symbol");

            var filteredStripeIds = reader.FilterStripes(sourceColumn, min: "CTSPillarNetworkA", max: "CTSPillarNetworkB");
            filteredStripeIds = reader.FilterStripes(filteredStripeIds, symbolColumn, min: "970", max: "973");

            Assert.Single(filteredStripeIds);
            Assert.Contains(filteredStripeIds, stripeId => stripeId == 0);

            var rowGroupIndexIds = reader.FilterRowGroups(stripeId: 0, sourceColumn, min: "CTSPillarNetworkA", max: "CTSPillarNetworkB");
            rowGroupIndexIds = reader.FilterRowGroups(rowGroupIndexIds, stripeId: 0, symbolColumn, min: "970", max: "973");

            Assert.Single(rowGroupIndexIds);
            Assert.Contains(rowGroupIndexIds, rowEntryIndex => rowEntryIndex == 0);
        }

        [Fact]
        public void Filter_Multiple_RowEntries()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var beginTime1 = (decimal)(new TimeSpan(09, 43, 20)).TotalSeconds;
            var endTime1 = (decimal)(new TimeSpan(09, 43, 21)).TotalSeconds;

            var beginTime2 = (decimal)(new TimeSpan(10, 43, 20)).TotalSeconds;
            var endTime2 = (decimal)(new TimeSpan(10, 43, 21)).TotalSeconds;

            // Columns
            var sourceColumn = reader.GetColumn("source");
            var symbolColumn = reader.GetColumn("symbol");
            var timeColumn = reader.GetColumn("time");

            var filteredStripeIds = reader.FilterStripes(sourceColumn, min: "CTSPillarNetworkB", max: "CTSPillarNetworkB");
            filteredStripeIds = reader.FilterStripes(filteredStripeIds, symbolColumn, min: "SPY", max: "SPY");
            filteredStripeIds = reader.FilterStripes(filteredStripeIds, timeColumn, min: $"{beginTime1}", max: $"{endTime1}");
            Assert.Single(filteredStripeIds);
            Assert.Contains(filteredStripeIds, stripeId => stripeId == 0);

            var rowGroupIndexIds = reader.FilterRowGroups(stripeId: 0, sourceColumn, min: "CTSPillarNetworkB", max: "CTSPillarNetworkB");
            rowGroupIndexIds = reader.FilterRowGroups(rowGroupIndexIds, stripeId: 0, symbolColumn, min: "SPY", max: "SPY");
            rowGroupIndexIds = reader.FilterRowGroups(rowGroupIndexIds, stripeId: 0, timeColumn, min: $"{beginTime1}", max: $"{endTime1}");
            Assert.Equal(2, rowGroupIndexIds.Count());
            Assert.Contains(rowGroupIndexIds, rowEntryIndex => rowEntryIndex == 0);
            Assert.Contains(rowGroupIndexIds, rowEntryIndex => rowEntryIndex == 1);

            rowGroupIndexIds = reader.FilterRowGroups(rowGroupIndexIds, stripeId: 0, timeColumn, min: $"{beginTime2}", max: $"{endTime2}");
            Assert.Empty(rowGroupIndexIds);

            filteredStripeIds = reader.FilterStripes(filteredStripeIds, timeColumn, min: $"{beginTime2}", max: $"{endTime2}");
            Assert.Empty(filteredStripeIds);
        }
    }
}
