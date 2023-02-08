using ApacheOrcDotNet.OptimizedReader;
using ApacheOrcDotNet.OptimizedReader.Buffers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Reader = ApacheOrcDotNet.OptimizedReader.OrcReader;

namespace ApacheOrcDotNet.OptimizedReaderTest.App
{
    public class Columns
    {
        public OrcColumn Vendor { get; init; }
        public OrcColumn Product { get; init; }
        public OrcColumn Time { get; init; }
        public OrcColumn Sales { get; init; }
    }

    public class Buffers
    {
        public BaseColumnBuffer<string> Vendor { get; init; }
        public BaseColumnBuffer<string> Product { get; init; }
        public BaseColumnBuffer<decimal?> Time { get; init; }
        public BaseColumnBuffer<long?> Sales { get; init; }
    }

    public class Lookup
    {
        public string Vendor { get; init; }
        public string Product { get; init; }
        public TimeSpan StartTime { get; set; }
        public TimeSpan EndTime { get; set; }
    }

    public class SampleDataSource
    {
        private readonly Reader _reader;
        private readonly Lookup _lookup;
        private readonly Columns _columns;
        private readonly Buffers _buffers;
        private readonly Dictionary<int, List<int>> _matchCandidates = new();

        public SampleDataSource(Reader reader, string vendor, string product)
        {
            _reader = reader;

            _lookup = new Lookup
            {
                Vendor = vendor,
                Product = product
            };

            _columns = new Columns
            {
                Vendor = _reader.GetColumn("stringDictionaryV2"),
                Product = _reader.GetColumn("stringDirectV2"),
                Time = _reader.GetColumn("decimal"),
                Sales = _reader.GetColumn("integer")
            };

            _buffers = new Buffers
            {
                Vendor = reader.CreateStringColumnBuffer(_columns.Vendor),
                Product = reader.CreateStringColumnBuffer(_columns.Product),
                Time = reader.CreateDecimalColumnBuffer(_columns.Time),
                Sales = reader.CreateIntegerColumnBuffer(_columns.Sales)
            };

            var vendorFilterValues = FilterValues.CreateFromString(min: vendor, max: vendor);
            var productFilterValues = FilterValues.CreateFromString(min: product, max: product);

            var stripeIds = _reader.FilterStripes(_columns.Vendor, vendorFilterValues);
            stripeIds = _reader.FilterStripes(stripeIds, _columns.Product, productFilterValues);

            foreach (var stripeId in stripeIds)
            {
                var rowGroupIndexes = _reader.FilterRowGroups(stripeId, _columns.Vendor, vendorFilterValues);
                rowGroupIndexes = _reader.FilterRowGroups(rowGroupIndexes, stripeId, _columns.Product, productFilterValues);

                if (rowGroupIndexes.Any())
                    _matchCandidates.Add(stripeId, rowGroupIndexes.ToList());
            }
        }

        public TimeRangeReader CreateTimeRangeReader(TimeSpan startTime, TimeSpan endTime)
        {
            _lookup.StartTime = startTime;
            _lookup.EndTime = endTime;

            return new TimeRangeReader(_reader, _lookup, _columns, _buffers, _matchCandidates);
        }
    }

    public class TimeRangeReader
    {
        private readonly Reader _reader;
        private readonly Lookup _lookup;
        private readonly Columns _columns;
        private readonly Buffers _buffers;
        private readonly Queue<(int stripeId, int rowGroupIndex)> _matches = new();
        private readonly Dictionary<int, List<int>> _matchCandidates = new();
        private bool _valuesFound;

        public TimeRangeReader(Reader reader, Lookup lookup, Columns columns, Buffers buffers, Dictionary<int, List<int>> matchCandidates)
        {
            _reader = reader;
            _lookup = lookup;
            _columns = columns;
            _buffers = buffers;
            _matchCandidates = matchCandidates;

            ApplyFilters();
        }

        public int ApproxRowCount => _matches.Count * _reader.MaxValuesPerRowGroup;

        public int ReadBatch(Span<decimal?> times, Span<long?> sales)
        {
            var writeIndex = 0;
            var startTimeTotalSeconds = (decimal)_lookup.StartTime.TotalSeconds;
            var endTimeTotalSeconds = (decimal)_lookup.EndTime.TotalSeconds;

            while (_matches.Count > 0)
            {
                var (stripeId, rowEntryIndex) = _matches.Dequeue();

                Task.WhenAll(
                    _reader.LoadDataAsync(stripeId, rowEntryIndex, _buffers.Vendor),
                    _reader.LoadDataAsync(stripeId, rowEntryIndex, _buffers.Product),
                    _reader.LoadDataAsync(stripeId, rowEntryIndex, _buffers.Time),
                    _reader.LoadDataAsync(stripeId, rowEntryIndex, _buffers.Sales)
                ).Wait();

                for (int idx = 0; idx < _reader.NumValuesLoaded; idx++)
                {
                    var vendor = _buffers.Vendor.Values[idx];
                    var product = _buffers.Product.Values[idx];
                    var time = _buffers.Time.Values[idx];

                    if (vendor == _lookup.Vendor && product == _lookup.Product && time >= startTimeTotalSeconds && time <= endTimeTotalSeconds)
                    {
                        _valuesFound = true;

                        times[writeIndex] = _buffers.Time.Values[idx];
                        sales[writeIndex] = _buffers.Sales.Values[idx];

                        writeIndex++;
                    }
                }

                // Only keep looking for values if nothing has been found yet.
                if (_valuesFound)
                    break;
            }

            return writeIndex;
        }

        public void Reset() => ApplyFilters();

        private void ApplyFilters()
        {
            _matches.Clear();
            _valuesFound = false;

            var timeFilterValue = FilterValues.CreateFromTime(min: _lookup.StartTime, max: _lookup.EndTime);
            var stripeIds = _reader.FilterStripes(_matchCandidates.Keys, _columns.Time, timeFilterValue);

            foreach (var stripeId in stripeIds)
            {
                var rowGroupIndexes = _reader.FilterRowGroups(_matchCandidates[stripeId], stripeId, _columns.Time, timeFilterValue).ToList();

                foreach (var rowGroupIndex in rowGroupIndexes)
                {
                    _matches.Enqueue((stripeId, rowGroupIndex));
                }
            }
        }
    }
}
