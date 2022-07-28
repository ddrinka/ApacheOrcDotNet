using ApacheOrcDotNet.OptimizedReader;
using ApacheOrcDotNet.OptimizedReader.Buffers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Reader = ApacheOrcDotNet.OptimizedReader.OrcReader;

namespace ApacheOrcDotNet.OptimizedReaderTest.App
{
    public class TradeDataSource
    {
        private readonly Reader _reader;
        private readonly string _souce;
        private readonly string _symbol;

        public TradeDataSource(Reader reader, string source, string symbol)
        {
            _reader = reader;
            _souce = source;
            _symbol = symbol;
        }

        public TimeRangeReader CreateTimeRangeReader(TimeSpan startTime, TimeSpan endTime)
            => new TimeRangeReader(_reader, _souce, _symbol, startTime, endTime);
    }

    public class TimeRangeReader
    {
        private readonly Reader _reader;
        private readonly string _souce;
        private readonly string _symbol;
        private readonly TimeSpan _startTime;
        private readonly TimeSpan _endTime;
        private readonly BaseColumnBuffer<string> _sourceColumnBuffer;
        private readonly BaseColumnBuffer<string> _symbolColumnBuffer;
        private readonly BaseColumnBuffer<decimal?> _timeColumnBuffer;
        private readonly BaseColumnBuffer<decimal?> _priceColumnBuffer;
        private readonly BaseColumnBuffer<long?> _sizeColumnBuffer;
        private readonly Dictionary<int, List<int>> _filters = new();

        public TimeRangeReader(Reader reader, string source, string symbol, TimeSpan startTime, TimeSpan endTime)
        {
            _reader = reader;
            _souce = source;
            _symbol = symbol;
            _startTime = startTime;
            _endTime = endTime;

            var sourceColumn = _reader.GetColumn("source");
            var symbolColumn = _reader.GetColumn("symbol");
            var timeColumn = _reader.GetColumn("time");
            var priceColumn = _reader.GetColumn("price");
            var sizeColumn = _reader.GetColumn("size");

            _sourceColumnBuffer = reader.CreateStringColumnBuffer(sourceColumn);
            _symbolColumnBuffer = reader.CreateStringColumnBuffer(symbolColumn);
            _timeColumnBuffer = reader.CreateDecimalColumnBuffer(timeColumn);
            _priceColumnBuffer = reader.CreateDecimalColumnBuffer(priceColumn);
            _sizeColumnBuffer = reader.CreateIntegerColumnBuffer(sizeColumn);

            var sourceFilterValues = FilterValues.CreateFromString(min: _souce, max: _souce);
            var symbolFilterValues = FilterValues.CreateFromString(min: _symbol, max: _symbol);
            var timeFilterValues = FilterValues.CreateFromTime(min: _startTime, max: _endTime);

            var stripeIds = _reader.FilterStripes(sourceColumn, sourceFilterValues);
            stripeIds = _reader.FilterStripes(stripeIds, symbolColumn, symbolFilterValues);
            stripeIds = _reader.FilterStripes(stripeIds, timeColumn, timeFilterValues);

            foreach (var stripeId in stripeIds)
            {
                var rowGroupIndexes = _reader.FilterRowGroups(stripeId, sourceColumn, sourceFilterValues);
                rowGroupIndexes = _reader.FilterRowGroups(rowGroupIndexes, stripeId, symbolColumn, symbolFilterValues);
                rowGroupIndexes = _reader.FilterRowGroups(rowGroupIndexes, stripeId, timeColumn, timeFilterValues);

                _filters.Add(stripeId, rowGroupIndexes.ToList());
            }
        }

        public int ApproxRowCount
        {
            get
            {
                var rowGroups = _filters.Values.Sum(v => v.Count);
                return rowGroups * _reader.MaxValuesPerRowGroup;
            }
        }

        public int ReadBatch(Span<decimal?> times, Span<decimal?> prices, Span<long?> sizes)
        {
            var numRows = 0;
            var startTimeTotalSeconds = (decimal)_startTime.TotalSeconds;
            var endTimeTotalSeconds = (decimal)_endTime.TotalSeconds;

            foreach (var (stripeId, rowGroupIndexes) in _filters)
            {
                foreach (var rowEntryIndex in rowGroupIndexes)
                {
                    Task.WhenAll(
                        _reader.LoadDataAsync(stripeId, rowEntryIndex, _sourceColumnBuffer),
                        _reader.LoadDataAsync(stripeId, rowEntryIndex, _symbolColumnBuffer),
                        _reader.LoadDataAsync(stripeId, rowEntryIndex, _timeColumnBuffer),
                        _reader.LoadDataAsync(stripeId, rowEntryIndex, _sizeColumnBuffer),
                        _reader.LoadDataAsync(stripeId, rowEntryIndex, _priceColumnBuffer)
                    ).Wait();

                    for (int idx = 0; idx < _reader.NumValuesLoaded; idx++)
                    {
                        var source = _sourceColumnBuffer.Values[idx];
                        var symbol = _symbolColumnBuffer.Values[idx];
                        var time = _timeColumnBuffer.Values[idx];

                        if (source == _souce && symbol == _symbol && time >= startTimeTotalSeconds && time <= endTimeTotalSeconds)
                        {
                            times[numRows] = _timeColumnBuffer.Values[idx];
                            sizes[numRows] = _sizeColumnBuffer.Values[idx];
                            prices[numRows] = _priceColumnBuffer.Values[idx];
                            numRows++;
                        }
                    }
                }
            }

            return numRows;
        }
    }
}
