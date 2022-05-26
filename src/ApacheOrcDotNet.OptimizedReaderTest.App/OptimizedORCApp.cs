using ApacheOrcDotNet.OptimizedReader;
using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using System;
using System.Diagnostics;
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
        private readonly string _orcFileName;
        private readonly OptimizedORCAppConfiguration _configuration;
        private readonly IByteRangeProviderFactory _byteRangeProviderFactory;

        public OptimizedORCApp(string orcFileName, OptimizedORCAppConfiguration confituration, IByteRangeProviderFactory byteRangeProviderFactory)
        {
            _orcFileName = orcFileName;
            _configuration = confituration;
            _byteRangeProviderFactory = byteRangeProviderFactory;
        }

        public void Run()
        {
            //
            var watch = new Stopwatch();
            var reader = new OptimizedReader.OrcReader(
                new OrcOptimizedReaderConfiguration() { OptimisticFileTailReadLength = 1 },
                _byteRangeProviderFactory.Create(_orcFileName)
            );

            watch.Start();

            // Args
            var lookupSource = _configuration.Source;
            var lookupSymbol = _configuration.Symbol;
            var beginTime = (decimal)_configuration.BeginTime.TotalSeconds;
            var endTime = (decimal)_configuration.EndTime.TotalSeconds;

            // Columns
            var sourceColumn = reader.CreateColumn("source", lookupSource, lookupSource);
            var symbolColumn = reader.CreateColumn("symbol", lookupSymbol, lookupSymbol);
            var timeColumn = reader.CreateColumn("time", $"{beginTime}", $"{endTime}");
            var sizeColumn = reader.CreateColumn("size");
            //var dateColumn = reader.CreateColumn("date");
            //var doubleColumn = reader.CreateColumn("double");
            //var floatColumn = reader.CreateColumn("float");
            //var timeStampColumn = reader.CreateColumn("timeStamp");
            //var binaryColumn = reader.CreateColumn("binary");
            //var byteColumn = reader.CreateColumn("byte");

            // Buffers
            var sourceColumnBuffer = reader.CreateStringColumnBuffer(sourceColumn);
            var symbolColumnBuffer = reader.CreateStringColumnBuffer(symbolColumn);
            var timeColumnBuffer = reader.CreateDecimalColumnReader(timeColumn);
            var sizeColumnBuffer = reader.CreateIntegerColumnBuffer(sizeColumn);
            //var dateColumnBuffer = reader.CreateDateColumnBuffer(dateColumn);
            //var doubleColumnBuffer = reader.CreateDoubleColumnBuffer(doubleColumn);
            //var floatColumnBuffer = reader.CreateFloatColumnBuffer(floatColumn);
            //var timeStampColumnBuffer = reader.CreateTimestampColumnBuffer(timeStampColumn);
            //var binaryColumnBuffer = reader.CreateBinaryColumnBuffer(binaryColumn);
            //var byteColumnBuffer = reader.CreateByteColumnBuffer(byteColumn);

            // Filters
            var stripeIds = reader.GetStripeIds(sourceColumn);
            stripeIds = reader.GetStripeIds(stripeIds, symbolColumn);
            stripeIds = reader.GetStripeIds(stripeIds, timeColumn);

            foreach (var stripeId in stripeIds)
            {
                var rowGroupIndexes = reader.GetRowGroupIndexes(stripeId, sourceColumn);
                rowGroupIndexes = reader.GetRowGroupIndexes(rowGroupIndexes, stripeId, symbolColumn);
                rowGroupIndexes = reader.GetRowGroupIndexes(rowGroupIndexes, stripeId, timeColumn);

                foreach (var rowEntryIndex in rowGroupIndexes)
                {
                    // Process
                    Parallel.Invoke(
                        () => reader.FillBuffer(stripeId, rowEntryIndex, sourceColumnBuffer)
                        , () => reader.FillBuffer(stripeId, rowEntryIndex, symbolColumnBuffer)
                        , () => reader.FillBuffer(stripeId, rowEntryIndex, timeColumnBuffer)
                        , () => reader.FillBuffer(stripeId, rowEntryIndex, sizeColumnBuffer)
                        //, () => reader.FillBuffer(stripeId, rowEntryIndex, dateColumnBuffer)
                        //, () => reader.FillBuffer(stripeId, rowEntryIndex, doubleColumnBuffer)
                        //, () => reader.FillBuffer(stripeId, rowEntryIndex, floatColumnBuffer)
                        //, () => reader.FillBuffer(stripeId, rowEntryIndex, timeStampColumnBuffer)
                        //, () => reader.FillBuffer(stripeId, rowEntryIndex, binaryColumnBuffer)
                        //, () => reader.FillBuffer(stripeId, rowEntryIndex, byteColumnBuffer)
                    );

                    for (int idx = 0; idx < sizeColumnBuffer.Values.Length; idx++)
                    {
                        var source = sourceColumnBuffer.Values[idx];
                        var symbol = symbolColumnBuffer.Values[idx];
                        var time = timeColumnBuffer.Values[idx];
                        var size = sizeColumnBuffer.Values[idx];
                        //var date = dateColumnBuffer.Values[idx];
                        //var dobl = doubleColumnBuffer.Values[idx];
                        //var sing = floatColumnBuffer.Values[idx];
                        //var timeStamp = timeStampColumnBuffer.Values[idx];
                        //var binary = binaryColumnBuffer.Values[idx];
                        //var tinyInt = byteColumnBuffer.Values[idx];

                        if (source == lookupSource && symbol == lookupSymbol && time >= beginTime && time <= endTime)
                        {
                            Console.WriteLine($"" +
                                $"{source}," +
                                $"{symbol}," +
                                $"{time.ToString().PadRight(15, '0')}," +
                                $"{size}" +
                                //$"     " +
                                //$"{(date.HasValue ? date.Value.ToString("MM/dd/yyyy") : string.Empty)}," +
                                //$"{dobl}," +
                                //$"{sing}," +
                                //$"{(timeStamp.HasValue ? timeStamp.Value.ToString("yyyy-MM-dd HH:mm:ss") : string.Empty)}," +
                                //$"{(binary != null ? Encoding.ASCII.GetString(binary) : string.Empty)}," +
                                //$"{tinyInt}" +
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
