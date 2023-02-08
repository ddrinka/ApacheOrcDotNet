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

            var dicColumn = reader.GetColumn("stringDictionaryV2");
            var dirColumn = reader.GetColumn("stringDirectV2");

            var dicfilterValues = FilterValues.CreateFromString(min: "abc", max: "xyz");
            var dirfilterValues = FilterValues.CreateFromString(min: "970", max: "973");

            var filteredStripeIds = reader.FilterStripes(dicColumn, dicfilterValues);
            filteredStripeIds = reader.FilterStripes(filteredStripeIds, dirColumn, dirfilterValues);

            Assert.Single(filteredStripeIds);
            Assert.Contains(filteredStripeIds, stripeId => stripeId == 0);

            var rowGroupIndexIds = reader.FilterRowGroups(stripeId: 0, dicColumn, dicfilterValues);
            rowGroupIndexIds = reader.FilterRowGroups(rowGroupIndexIds, stripeId: 0, dirColumn, dirfilterValues);

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
            var sourceColumn = reader.GetColumn("stringDictionaryV2");
            var symbolColumn = reader.GetColumn("stringDirectV2");
            var timeColumn = reader.GetColumn("decimal");

            var sourcefilterValues = FilterValues.CreateFromString(min: "xyz", max: "xyz");
            var symbolfilterValues = FilterValues.CreateFromString(min: "test", max: "test");
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
        public void Filter_Unsorted_Date()
        {
            var config = new OrcReaderConfiguration();
            var reader = new OrcReader(config, _byteRangeProvider);

            // Columns
            var dateColumn = reader.GetColumn("date");

            var datefilterValues = FilterValues.CreateFromDate(min: new DateTime(1997, 5, 17), max: new DateTime(1997, 5, 19));

            var filteredStripeIds = reader.FilterStripes(dateColumn, datefilterValues);
            Assert.Single(filteredStripeIds);
            Assert.Contains(filteredStripeIds, stripeId => stripeId == 0);

            var rowGroupIndexIds = reader.FilterRowGroups(stripeId: 0, dateColumn, datefilterValues);
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

            var timestampfilterValues = FilterValues.CreateFromTimestamp(min: new DateTime(2015, 1, 1, 2, 46, 35), max: new DateTime(2015, 1, 1, 2, 46, 47));

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

            var timestampfilterValues = FilterValues.CreateFromTimestamp(min: new DateTime(2015, 1, 1, 2, 46, 47), max: new DateTime(2015, 1, 1, 2, 46, 47));

            var filteredStripeIds = reader.FilterStripes(timestampColumn, timestampfilterValues);
            Assert.Single(filteredStripeIds);
            Assert.Contains(filteredStripeIds, stripeId => stripeId == 0);

            var rowGroupIndexIds = reader.FilterRowGroups(stripeId: 0, timestampColumn, timestampfilterValues);
            Assert.Single(rowGroupIndexIds);
            Assert.Contains(rowGroupIndexIds, rowEntryIndex => rowEntryIndex == 1);
        }
    }
}
