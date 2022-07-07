using ApacheOrcDotNet.OptimizedReader;
using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using System;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReaderTest.App
{
    public class OptimizedORCAppConfiguration
    {
        public DateTime Date { get; set; }
        public string Source { get; set; }
        public string Symbol { get; set; }
        public TimeSpan BeginTime { get; set; }
        public TimeSpan EndTime { get; set; }
    }

    public class OptimizedORCApp
    {
        private readonly string _orcFileUri;
        private readonly OptimizedORCAppConfiguration _configuration;
        private readonly IByteRangeProviderFactory _byteRangeProviderFactory;

        public OptimizedORCApp(string orcFileUri, OptimizedORCAppConfiguration confituration, IByteRangeProviderFactory byteRangeProviderFactory)
        {
            _orcFileUri = orcFileUri;
            _configuration = confituration;
            _byteRangeProviderFactory = byteRangeProviderFactory;
        }

        public async Task Run()
        {
            //
            var watch = new Stopwatch();
            var configs = new OrcReaderConfiguration();
            var rangeProvider = _byteRangeProviderFactory.Create(_orcFileUri);
            var reader = new OptimizedReader.OrcReader(configs, rangeProvider);

            watch.Start();

            // Args
            var lookupSource = _configuration.Source;
            var lookupSymbol = _configuration.Symbol;
            var beginTime = _configuration.BeginTime;
            var endTime = _configuration.EndTime;

            // Columns
            var sourceColumn = reader.GetColumn("source");
            var symbolColumn = reader.GetColumn("symbol");
            var timeColumn = reader.GetColumn("time");
            var sizeColumn = reader.GetColumn("size");
            var dateColumn = reader.GetColumn("date");
            var doubleColumn = reader.GetColumn("double");
            var floatColumn = reader.GetColumn("float");
            var timeStampColumn = reader.GetColumn("timeStamp");
            var binaryColumn = reader.GetColumn("binary");
            var byteColumn = reader.GetColumn("byte");
            var booleanColumn = reader.GetColumn("boolean");

            // Buffers
            var sourceColumnBuffer = reader.CreateStringColumnBuffer(sourceColumn);
            var symbolColumnBuffer = reader.CreateStringColumnBuffer(symbolColumn);
            var timeColumnBuffer = reader.CreateDecimalColumnBuffer(timeColumn);
            var sizeColumnBuffer = reader.CreateIntegerColumnBuffer(sizeColumn);
            var dateColumnBuffer = reader.CreateDateColumnBuffer(dateColumn);
            var doubleColumnBuffer = reader.CreateDoubleColumnBuffer(doubleColumn);
            var floatColumnBuffer = reader.CreateFloatColumnBuffer(floatColumn);
            var timeStampColumnBuffer = reader.CreateTimestampColumnBuffer(timeStampColumn);
            var binaryColumnBuffer = reader.CreateBinaryColumnBuffer(binaryColumn);
            var byteColumnBuffer = reader.CreateByteColumnBuffer(byteColumn);
            var booleanColumnBuffer = reader.CreateBooleanColumnReader(booleanColumn);

            // Filters
            var sourceFilterValues = FilterValues.CreateFromString(min: lookupSource, max: lookupSource);
            var symbolFilterValues = FilterValues.CreateFromString(min: lookupSymbol, max: lookupSymbol);
            var timeFilterValues = FilterValues.CreateFromTime(min: beginTime, max: endTime);

            //
            var stripeIds = reader.FilterStripes(sourceColumn, sourceFilterValues);
            stripeIds = reader.FilterStripes(stripeIds, symbolColumn, symbolFilterValues);
            stripeIds = reader.FilterStripes(stripeIds, timeColumn, timeFilterValues);

            foreach (var stripeId in stripeIds)
            {
                //
                var rowGroupIndexes = reader.FilterRowGroups(stripeId, sourceColumn, sourceFilterValues);
                rowGroupIndexes = reader.FilterRowGroups(rowGroupIndexes, stripeId, symbolColumn, symbolFilterValues);
                rowGroupIndexes = reader.FilterRowGroups(rowGroupIndexes, stripeId, timeColumn, timeFilterValues);

                foreach (var rowEntryIndex in rowGroupIndexes)
                {
                    await Task.WhenAll(
                        reader.LoadDataAsync(stripeId, rowEntryIndex, sourceColumnBuffer),
                        reader.LoadDataAsync(stripeId, rowEntryIndex, symbolColumnBuffer),
                        reader.LoadDataAsync(stripeId, rowEntryIndex, timeColumnBuffer),
                        reader.LoadDataAsync(stripeId, rowEntryIndex, sizeColumnBuffer),
                        reader.LoadDataAsync(stripeId, rowEntryIndex, dateColumnBuffer),
                        reader.LoadDataAsync(stripeId, rowEntryIndex, doubleColumnBuffer),
                        reader.LoadDataAsync(stripeId, rowEntryIndex, floatColumnBuffer),
                        reader.LoadDataAsync(stripeId, rowEntryIndex, timeStampColumnBuffer),
                        reader.LoadDataAsync(stripeId, rowEntryIndex, binaryColumnBuffer),
                        reader.LoadDataAsync(stripeId, rowEntryIndex, byteColumnBuffer),
                        reader.LoadDataAsync(stripeId, rowEntryIndex, booleanColumnBuffer)
                    );

                    for (int idx = 0; idx < reader.NumValues; idx++)
                    {
                        var source = sourceColumnBuffer.Values[idx];
                        var symbol = symbolColumnBuffer.Values[idx];
                        var time = timeColumnBuffer.Values[idx];
                        var size = sizeColumnBuffer.Values[idx];
                        var date = dateColumnBuffer.Values[idx];
                        var dobl = doubleColumnBuffer.Values[idx];
                        var sing = floatColumnBuffer.Values[idx];
                        var timeStamp = timeStampColumnBuffer.Values[idx];
                        var binary = binaryColumnBuffer.Values[idx];
                        var tinyInt = byteColumnBuffer.Values[idx];
                        var boolean = booleanColumnBuffer.Values[idx];

                        if (source == lookupSource && symbol == lookupSymbol && time >= (decimal)beginTime.TotalSeconds && time <= (decimal)endTime.TotalSeconds)
                        {
                            Console.WriteLine($"" +
                                $"{source}," +
                                $"{symbol}," +
                                $"{time.ToString().PadRight(15, '0')}," +
                                $"{size}" +
                                $"     " +
                                $"{(date.HasValue ? date.Value.ToString("MM/dd/yyyy") : string.Empty)}," +
                                $"{dobl}," +
                                $"{sing}," +
                                $"{(timeStamp.HasValue ? timeStamp.Value.ToString("yyyy-MM-dd HH:mm:ss") : string.Empty)}," +
                                $"{(binary != null ? Encoding.ASCII.GetString(binary) : string.Empty)}," +
                                $"{tinyInt}," +
                                $"{boolean}" +
                                $""
                            );
                        }
                    }
                }
            }

            watch.Stop();
            Console.WriteLine();
            Console.WriteLine($"Read execution time: {watch.Elapsed}");
        }
    }
}
