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

            var sourcefilterValues = FilterValues.CreateFromString(min: "CTSPillarNetworkA", max: "CTSPillarNetworkB");
            var symbolfilterValues = FilterValues.CreateFromString(min: "970", max: "973");

            var filteredStripeIds = reader.FilterStripes(sourceColumn, sourcefilterValues);
            filteredStripeIds = reader.FilterStripes(filteredStripeIds, symbolColumn, symbolfilterValues);

            Assert.Single(filteredStripeIds);
            Assert.Contains(filteredStripeIds, stripeId => stripeId == 0);

            var rowGroupIndexIds = reader.FilterRowGroups(stripeId: 0, sourceColumn, sourcefilterValues);
            rowGroupIndexIds = reader.FilterRowGroups(rowGroupIndexIds, stripeId: 0, symbolColumn, symbolfilterValues);

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

            var sourcefilterValues = FilterValues.CreateFromString(min: "CTSPillarNetworkB", max: "CTSPillarNetworkB");
            var symbolfilterValues = FilterValues.CreateFromString(min: "SPY", max: "SPY");
            var timefilterValues1 = FilterValues.CreateFromTime(min: beginTime1, max: endTime1);
            var timefilterValues2 = FilterValues.CreateFromTime(min: beginTime2, max: endTime2);

            var filteredStripeIds = reader.FilterStripes(sourceColumn, sourcefilterValues);
            filteredStripeIds = reader.FilterStripes(filteredStripeIds, symbolColumn, symbolfilterValues);
            filteredStripeIds = reader.FilterStripes(filteredStripeIds, timeColumn, timefilterValues1);
            Assert.Single(filteredStripeIds);
            Assert.Contains(filteredStripeIds, stripeId => stripeId == 0);

            var rowGroupIndexIds = reader.FilterRowGroups(stripeId: 0, sourceColumn, sourcefilterValues);
            rowGroupIndexIds = reader.FilterRowGroups(rowGroupIndexIds, stripeId: 0, symbolColumn, symbolfilterValues);
            rowGroupIndexIds = reader.FilterRowGroups(rowGroupIndexIds, stripeId: 0, timeColumn, timefilterValues1);
            Assert.Equal(2, rowGroupIndexIds.Count());
            Assert.Contains(rowGroupIndexIds, rowEntryIndex => rowEntryIndex == 0);
            Assert.Contains(rowGroupIndexIds, rowEntryIndex => rowEntryIndex == 1);

            rowGroupIndexIds = reader.FilterRowGroups(rowGroupIndexIds, stripeId: 0, timeColumn, timefilterValues2);
            Assert.Empty(rowGroupIndexIds);

            filteredStripeIds = reader.FilterStripes(filteredStripeIds, timeColumn, timefilterValues2);
            Assert.Empty(filteredStripeIds);
        }

        [Fact]
        public void Filter_Unsorted_Date_Multiple()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            // Columns
            var dateColumn = reader.GetColumn("date");

            var datefilterValues = FilterValues.CreateFromDate(min: new DateTime(1997, 5, 17), max: new DateTime(1997, 5, 20));

            var filteredStripeIds = reader.FilterStripes(dateColumn, datefilterValues);
            Assert.Single(filteredStripeIds);
            Assert.Contains(filteredStripeIds, stripeId => stripeId == 0);

            var rowGroupIndexIds = reader.FilterRowGroups(stripeId: 0, dateColumn, datefilterValues);
            Assert.Equal(2, rowGroupIndexIds.Count());
            Assert.Contains(rowGroupIndexIds, rowEntryIndex => rowEntryIndex == 0);
            Assert.Contains(rowGroupIndexIds, rowEntryIndex => rowEntryIndex == 1);
        }

        [Fact]
        public void Filter_Unsorted_Date_Single()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            // Columns
            var dateColumn = reader.GetColumn("date");

            var datefilterValues = FilterValues.CreateFromDate(min: new DateTime(1997, 5, 20), max: new DateTime(1997, 5, 20));

            var filteredStripeIds = reader.FilterStripes(dateColumn, datefilterValues);
            Assert.Single(filteredStripeIds);
            Assert.Contains(filteredStripeIds, stripeId => stripeId == 0);

            var rowGroupIndexIds = reader.FilterRowGroups(stripeId: 0, dateColumn, datefilterValues);
            Assert.Single(rowGroupIndexIds);
            Assert.Contains(rowGroupIndexIds, rowEntryIndex => rowEntryIndex == 1);
        }

        [Fact]
        public void Filter_Unsorted_Timestamp_Multiple()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            // Columns
            var timestampColumn = reader.GetColumn("timestamp");

            var timestampfilterValues = FilterValues.CreateFromTimestamp(min: new DateTime(2015, 1, 1, 2, 46, 35), max: new DateTime(2015, 1, 1, 2, 46, 41));

            var filteredStripeIds = reader.FilterStripes(timestampColumn, timestampfilterValues);
            Assert.Single(filteredStripeIds);
            Assert.Contains(filteredStripeIds, stripeId => stripeId == 0);

            var rowGroupIndexIds = reader.FilterRowGroups(stripeId: 0, timestampColumn, timestampfilterValues);
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

            var timestampfilterValues = FilterValues.CreateFromTimestamp(min: new DateTime(2015, 1, 1, 2, 46, 41), max: new DateTime(2015, 1, 1, 2, 46, 41));

            var filteredStripeIds = reader.FilterStripes(timestampColumn, timestampfilterValues);
            Assert.Single(filteredStripeIds);
            Assert.Contains(filteredStripeIds, stripeId => stripeId == 0);

            var rowGroupIndexIds = reader.FilterRowGroups(stripeId: 0, timestampColumn, timestampfilterValues);
            Assert.Single(rowGroupIndexIds);
            Assert.Contains(rowGroupIndexIds, rowEntryIndex => rowEntryIndex == 1);
        }
    }
}
