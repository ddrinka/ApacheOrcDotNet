using ApacheOrcDotNet.OptimizedReader;
using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using System;
using System.Text;

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
            var reader = new OrcOptimizedReader(
                new OrcOptimizedReaderConfiguration() { OptimisticFileTailReadLength = 1 },
                _byteRangeProviderFactory.Create(_orcFileName)
            );

            //
            var lookupSource = _configuration.Source;
            var lookupSymbol = _configuration.Symbol;
            var beginTime = (decimal)_configuration.BeginTime.TotalSeconds;
            var endTime = (decimal)_configuration.EndTime.TotalSeconds;

            //
            var stripeIds = reader.GetStripeIds("source", lookupSource, lookupSource);
            stripeIds = reader.GetStripeIds(stripeIds, "symbol", lookupSymbol, lookupSymbol);
            stripeIds = reader.GetStripeIds(stripeIds, "time", $"{beginTime}", $"{endTime}");

            foreach (var stripeId in stripeIds)
            {
                var rowGroupIndexes = reader.GetRowGroupIndexes(stripeId, "source", lookupSource, lookupSource);
                rowGroupIndexes = reader.GetRowGroupIndexes(rowGroupIndexes, stripeId, "symbol", lookupSymbol, lookupSymbol);
                rowGroupIndexes = reader.GetRowGroupIndexes(rowGroupIndexes, stripeId, "time", $"{beginTime}", $"{endTime}");

                foreach (var rowEntryIndex in rowGroupIndexes)
                {
                    var sourceReader = reader.CreateStringColumnReader(stripeId, rowEntryIndex, "source");
                    var symbolReader = reader.CreateStringColumnReader(stripeId, rowEntryIndex, "symbol");
                    var timeReader = reader.CreateDecimalColumnReaderAsDouble(stripeId, rowEntryIndex, "time");
                    var sizeReader = reader.CreateIntegerColumnReader(stripeId, rowEntryIndex, "size");
                    var dateReader = reader.CreateDateColumnReader(stripeId, rowEntryIndex, "date");
                    var doubleReader = reader.CreateDoubleColumnReader(stripeId, rowEntryIndex, "double");
                    var floatReader = reader.CreateFloatColumnReader(stripeId, rowEntryIndex, "float");
                    var timeStampReader = reader.CreateTimestampColumnReader(stripeId, rowEntryIndex, "timeStamp");
                    var binaryReader = reader.CreateBinaryColumnReader(stripeId, rowEntryIndex, "binary");
                    var byteReader = reader.CreateByteColumnReader(stripeId, rowEntryIndex, "byte");

                    sourceReader.FillBuffer();
                    symbolReader.FillBuffer();
                    timeReader.FillBuffer();
                    sizeReader.FillBuffer();
                    dateReader.FillBuffer();
                    doubleReader.FillBuffer();
                    floatReader.FillBuffer();
                    timeStampReader.FillBuffer();
                    binaryReader.FillBuffer();
                    byteReader.FillBuffer();

                    //foreach (var item in dateReader.Values)
                    //    Console.WriteLine(item.HasValue ? item.Value.ToString("yyyy-MM-dd") : "");

                    //var x = true;
                    //if (x)
                    //    continue;

                    //Parallel.Invoke(
                    //    () => sourceReader.FillBuffer(),
                    //    () => symbolReader.FillBuffer(),
                    //    () => timeReader.FillBuffer(),
                    //    () => sizeReader.FillBuffer()

                    //    () => dateReader.FillBuffer()
                    //    () => doubleReader.FillBuffer(),
                    //    () => timeStampReader.FillBuffer(),
                    //    () => binaryReader.FillBuffer(),
                    //    () => byteReader.FillBuffer()
                    //);

                    for (int idx = 0; idx < sizeReader.Values.Length; idx++)
                    {
                        var source = sourceReader.Values[idx];
                        var symbol = symbolReader.Values[idx];
                        var time = timeReader.Values[idx];
                        var size = sizeReader.Values[idx];

                        var date = dateReader.Values[idx];
                        var dobl = doubleReader.Values[idx];
                        var sing = floatReader.Values[idx];
                        var timeStamp = timeStampReader.Values[idx];
                        var binary = binaryReader.Values[idx];
                        var tinyInt = byteReader.Values[idx];

                        if (source == lookupSource && symbol == lookupSymbol && time >= (double)beginTime && time <= (double)endTime)
                        {
                            Console.WriteLine($"" +
                                $"{source}," +
                                $"{symbol}," +
                                $"{time.ToString().PadRight(15, '0')}," +
                                $"{size,-7}" +
                                $" |" +
                                $"{(date.HasValue ? date.Value.ToString("MM/dd/yyyy") : string.Empty)}," +
                                $"{dobl}," +
                                $"{sing}," +
                                $"{(timeStamp.HasValue ? timeStamp.Value.ToString("yyyy-MM-dd HH:mm:ss") : string.Empty)}," +
                                $"{(binary != null ? Encoding.ASCII.GetString(binary) : string.Empty)}," +
                                $"{tinyInt}" +
                                $""
                            );
                        }
                    }
                }
            }
        }
    }
}
