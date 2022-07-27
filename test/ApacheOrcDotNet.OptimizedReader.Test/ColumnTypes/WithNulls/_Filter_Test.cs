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

            sourceColumn.SetStringFilter(min: "CTSPillarNetworkA", max: "CTSPillarNetworkB");
            symbolColumn.SetStringFilter(min: "970", max: "973");

            var filteredStripeIds = reader.FilterStripes(sourceColumn);
            filteredStripeIds = reader.FilterStripes(filteredStripeIds, symbolColumn);

            Assert.Single(filteredStripeIds);
            Assert.Contains(filteredStripeIds, stripeId => stripeId == 0);

            var rowGroupIndexIds = reader.FilterRowGroups(stripeId: 0, sourceColumn);
            rowGroupIndexIds = reader.FilterRowGroups(rowGroupIndexIds, stripeId: 0, symbolColumn);

            Assert.Single(rowGroupIndexIds);
            Assert.Contains(rowGroupIndexIds, rowEntryIndex => rowEntryIndex == 0);
        }

        [Fact]
        public void Filter_Multiple_RowEntries()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            var beginTime1 = (new TimeSpan(09, 43, 20));
            var endTime1 = (new TimeSpan(09, 43, 21));

            var beginTime2 = (new TimeSpan(10, 43, 20));
            var endTime2 = (new TimeSpan(10, 43, 21));

            // Columns
            var sourceColumn = reader.GetColumn("source");
            var symbolColumn = reader.GetColumn("symbol");
            var timeColumn = reader.GetColumn("time");

            sourceColumn.SetStringFilter(min: "CTSPillarNetworkB", max: "CTSPillarNetworkB");
            symbolColumn.SetStringFilter(min: "SPY", max: "SPY");

            //
            timeColumn.SetTimeFilter(min: beginTime1, max: endTime1);

            var filteredStripeIds = reader.FilterStripes(sourceColumn);
            filteredStripeIds = reader.FilterStripes(filteredStripeIds, symbolColumn);
            filteredStripeIds = reader.FilterStripes(filteredStripeIds, timeColumn);
            Assert.Single(filteredStripeIds);
            Assert.Contains(filteredStripeIds, stripeId => stripeId == 0);

            var rowGroupIndexIds = reader.FilterRowGroups(stripeId: 0, sourceColumn);
            rowGroupIndexIds = reader.FilterRowGroups(rowGroupIndexIds, stripeId: 0, symbolColumn);
            rowGroupIndexIds = reader.FilterRowGroups(rowGroupIndexIds, stripeId: 0, timeColumn);
            Assert.Equal(2, rowGroupIndexIds.Count());
            Assert.Contains(rowGroupIndexIds, rowEntryIndex => rowEntryIndex == 0);
            Assert.Contains(rowGroupIndexIds, rowEntryIndex => rowEntryIndex == 1);

            //
            timeColumn.SetTimeFilter(min: beginTime2, max: endTime2);

            rowGroupIndexIds = reader.FilterRowGroups(rowGroupIndexIds, stripeId: 0, timeColumn);
            Assert.Empty(rowGroupIndexIds);

            filteredStripeIds = reader.FilterStripes(filteredStripeIds, timeColumn);
            Assert.Empty(filteredStripeIds);
        }

        [Fact]
        public void Filter_Unsorted_Date()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            // Columns
            var dateColumn = reader.GetColumn("date");

            dateColumn.SetDateFilter(min: new(1997, 5, 17), max: new(1997, 5, 19));

            var filteredStripeIds = reader.FilterStripes(dateColumn);
            Assert.Single(filteredStripeIds);
            Assert.Contains(filteredStripeIds, stripeId => stripeId == 0);

            var rowGroupIndexIds = reader.FilterRowGroups(stripeId: 0, dateColumn);
            Assert.Single(rowGroupIndexIds);
            Assert.Contains(rowGroupIndexIds, rowEntryIndex => rowEntryIndex == 0);
        }

        [Fact]
        public void Filter_Unsorted_Timestamp_Multiple()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            // Columns
            var timestampColumn = reader.GetColumn("timestamp");

            timestampColumn.SetTimestampFilter(min: new(2015, 1, 1, 2, 46, 35), max: new(2015, 1, 1, 2, 46, 47));

            var filteredStripeIds = reader.FilterStripes(timestampColumn);
            Assert.Single(filteredStripeIds);
            Assert.Contains(filteredStripeIds, stripeId => stripeId == 0);

            var rowGroupIndexIds = reader.FilterRowGroups(stripeId: 0, timestampColumn);
            Assert.Equal(2, rowGroupIndexIds.Count());
            Assert.Contains(rowGroupIndexIds, rowEntryIndex => rowEntryIndex == 0);
            Assert.Contains(rowGroupIndexIds, rowEntryIndex => rowEntryIndex == 1);
        }

        [Fact]
        public void Filter_Unsorted_Timestamp_Single()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            // Columns
            var timestampColumn = reader.GetColumn("timestamp");

            timestampColumn.SetTimestampFilter(min: new(2015, 1, 1, 2, 46, 47), max: new(2015, 1, 1, 2, 46, 47));

            var filteredStripeIds = reader.FilterStripes(timestampColumn);
            Assert.Single(filteredStripeIds);
            Assert.Contains(filteredStripeIds, stripeId => stripeId == 0);

            var rowGroupIndexIds = reader.FilterRowGroups(stripeId: 0, timestampColumn);
            Assert.Single(rowGroupIndexIds);
            Assert.Contains(rowGroupIndexIds, rowEntryIndex => rowEntryIndex == 1);
        }
    }
}
