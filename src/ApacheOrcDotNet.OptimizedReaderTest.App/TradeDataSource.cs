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
        public OrcColumn Source { get; init; }
        public OrcColumn Symbol { get; init; }
        public OrcColumn Time { get; init; }
        public OrcColumn Price { get; init; }
        public OrcColumn Size { get; init; }
    }

    public class Buffers
    {
        public BaseColumnBuffer<string> Source { get; init; }
        public BaseColumnBuffer<string> Symbol { get; init; }
        public BaseColumnBuffer<decimal?> Time { get; init; }
        public BaseColumnBuffer<decimal?> Price { get; init; }
        public BaseColumnBuffer<long?> Size { get; init; }
    }

    public class Lookup
    {
        public string Source { get; init; }
        public string Symbol { get; init; }
        public TimeSpan StartTime { get; set; }
        public TimeSpan EndTime { get; set; }
    }

    public class TradeDataSource
    {
        private readonly Reader _reader;
        private readonly Lookup _lookup;
        private readonly Columns _columns;
        private readonly Buffers _buffers;
        private readonly Dictionary<int, List<int>> _matchCandidates = new();

        public TradeDataSource(Reader reader, string source, string symbol)
        {
            _reader = reader;

            _lookup = new Lookup
            {
                Source = source,
                Symbol = symbol
            };

            _columns = new Columns
            {
                Source = _reader.GetColumn("source"),
                Symbol = _reader.GetColumn("symbol"),
                Time = _reader.GetColumn("time"),
                Price = _reader.GetColumn("price"),
                Size = _reader.GetColumn("size")
            };

            _buffers = new Buffers
            {
                Source = reader.CreateStringColumnBuffer(_columns.Source),
                Symbol = reader.CreateStringColumnBuffer(_columns.Symbol),
                Time = reader.CreateDecimalColumnBuffer(_columns.Time),
                Price = reader.CreateDecimalColumnBuffer(_columns.Price),
                Size = reader.CreateIntegerColumnBuffer(_columns.Size)
            };

            var sourceFilterValues = FilterValues.CreateFromString(min: source, max: source);
            var symbolFilterValues = FilterValues.CreateFromString(min: symbol, max: symbol);

            var stripeIds = _reader.FilterStripes(_columns.Source, sourceFilterValues);
            stripeIds = _reader.FilterStripes(stripeIds, _columns.Symbol, symbolFilterValues);

            foreach (var stripeId in stripeIds)
            {
                var rowGroupIndexes = _reader.FilterRowGroups(stripeId, _columns.Source, sourceFilterValues);
                rowGroupIndexes = _reader.FilterRowGroups(rowGroupIndexes, stripeId, _columns.Symbol, symbolFilterValues);

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

        public int ReadBatch(int writeStartIndex, Span<decimal?> times, Span<decimal?> prices, Span<long?> sizes)
        {
            var writeIndex = 0;
            var startTimeTotalSeconds = (decimal)_lookup.StartTime.TotalSeconds;
            var endTimeTotalSeconds = (decimal)_lookup.EndTime.TotalSeconds;

            while (_matches.Count > 0)
            {
                var (stripeId, rowEntryIndex) = _matches.Dequeue();

                Task.WhenAll(
                    _reader.LoadDataAsync(stripeId, rowEntryIndex, _buffers.Source),
                    _reader.LoadDataAsync(stripeId, rowEntryIndex, _buffers.Symbol),
                    _reader.LoadDataAsync(stripeId, rowEntryIndex, _buffers.Time),
                    _reader.LoadDataAsync(stripeId, rowEntryIndex, _buffers.Size),
                    _reader.LoadDataAsync(stripeId, rowEntryIndex, _buffers.Price)
                ).Wait();

                for (int idx = 0; idx < _reader.NumValuesLoaded; idx++)
                {
                    var source = _buffers.Source.Values[idx];
                    var symbol = _buffers.Symbol.Values[idx];
                    var time = _buffers.Time.Values[idx];

                    if (source == _lookup.Source && symbol == _lookup.Symbol && time >= startTimeTotalSeconds && time <= endTimeTotalSeconds)
                    {
                        _valuesFound = true;

                        times[writeStartIndex + writeIndex] = _buffers.Time.Values[idx];
                        sizes[writeStartIndex + writeIndex] = _buffers.Size.Values[idx];
                        prices[writeStartIndex + writeIndex] = _buffers.Price.Values[idx];

                        writeIndex++;
                    }
                }

                // We only keep looking for values if
                // nothing has been found before.
                if (writeIndex == 0 && !_valuesFound)
                    continue;

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
