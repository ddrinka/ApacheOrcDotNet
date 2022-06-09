using System;
using System.Linq;
using Xunit;

namespace ApacheOrcDotNet.OptimizedReader.Test.ColumnTypes.WithNulls
{
    public class _Filter_Test : _BaseColumnTypeWithNulls
    {
        [Fact]
        public void Filter_Single_RowEntry()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var sourceColumn = reader.GetColumn("source");
            var symbolColumn = reader.GetColumn("symbol");

            var filteredStripeIds = reader.GetStripeIds(sourceColumn, "CTSPillarNetworkA", "CTSPillarNetworkB");
            filteredStripeIds = reader.GetStripeIds(filteredStripeIds, symbolColumn, "970", "973");

            Assert.Single(filteredStripeIds);
            Assert.Contains(filteredStripeIds, stripeId => stripeId == 0);

            var rowGroupIndexes = reader.GetRowGroupIndexes(stripeId: 0, sourceColumn, "CTSPillarNetworkA", "CTSPillarNetworkB");
            rowGroupIndexes = reader.GetRowGroupIndexes(rowGroupIndexes, stripeId: 0, symbolColumn, "970", "973");

            Assert.Single(rowGroupIndexes);
            Assert.Contains(rowGroupIndexes, rowEntryIndex => rowEntryIndex == 0);
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

            var filteredStripeIds = reader.GetStripeIds(sourceColumn, "CTSPillarNetworkB", "CTSPillarNetworkB");
            filteredStripeIds = reader.GetStripeIds(filteredStripeIds, symbolColumn, "SPY", "SPY");
            filteredStripeIds = reader.GetStripeIds(filteredStripeIds, timeColumn, $"{beginTime1}", $"{endTime1}");
            Assert.Single(filteredStripeIds);
            Assert.Contains(filteredStripeIds, stripeId => stripeId == 0);

            var rowGroupIndexes = reader.GetRowGroupIndexes(stripeId: 0, sourceColumn, "CTSPillarNetworkB", "CTSPillarNetworkB");
            rowGroupIndexes = reader.GetRowGroupIndexes(rowGroupIndexes, stripeId: 0, symbolColumn, "SPY", "SPY");
            rowGroupIndexes = reader.GetRowGroupIndexes(rowGroupIndexes, stripeId: 0, timeColumn, $"{beginTime1}", $"{endTime1}");
            Assert.Equal(2, rowGroupIndexes.Count());
            Assert.Contains(rowGroupIndexes, rowEntryIndex => rowEntryIndex == 0);
            Assert.Contains(rowGroupIndexes, rowEntryIndex => rowEntryIndex == 1);

            rowGroupIndexes = reader.GetRowGroupIndexes(rowGroupIndexes, stripeId: 0, timeColumn, $"{beginTime2}", $"{endTime2}");
            Assert.Empty(rowGroupIndexes);

            filteredStripeIds = reader.GetStripeIds(filteredStripeIds, timeColumn, $"{beginTime2}", $"{endTime2}");
            Assert.Empty(filteredStripeIds);
        }
    }
}
