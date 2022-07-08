using ApacheOrcDotNet.OptimizedReader;
using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using System;
using System.Diagnostics;

namespace ApacheOrcDotNet.OptimizedReaderTest.App
{
    public class TradeDataSourceApp
    {
        private readonly string _orcFileUri;
        private readonly OptimizedORCAppConfiguration _configuration;
        private readonly IByteRangeProviderFactory _byteRangeProviderFactory;

        public TradeDataSourceApp(string orcFileUri, OptimizedORCAppConfiguration confituration, IByteRangeProviderFactory byteRangeProviderFactory)
        {
            _orcFileUri = orcFileUri;
            _configuration = confituration;
            _byteRangeProviderFactory = byteRangeProviderFactory;
        }

        public void Run()
        {
            //
            var watch = new Stopwatch();
            var configs = new OrcReaderConfiguration();
            var rangeProvider = _byteRangeProviderFactory.Create(_orcFileUri);
            var reader = new OptimizedReader.OrcReader(configs, rangeProvider);

            watch.Start();

            var timeRanges = new[] { (43200, 46800), (50400, 54000) };
            var symbolData = new TradeDataSource(reader, _configuration.Source, _configuration.Symbol);

            foreach (var (sTime, eTime) in timeRanges)
            {
                var timeRangeReader = symbolData.CreateTimeRangeReader(TimeSpan.FromSeconds(sTime), TimeSpan.FromSeconds(eTime));

                var approxRowCount = timeRangeReader.ApproxRowCount;

                var times = new decimal?[approxRowCount];
                var prices = new decimal?[approxRowCount];
                var sizes = new long?[approxRowCount];

                int numRows = timeRangeReader.ReadBatch(times, prices, sizes);

                Console.WriteLine();
                Console.WriteLine($"Read {numRows} rows of data");
            }

            watch.Stop();
            Console.WriteLine();
            Console.WriteLine($"Read execution time: {watch.Elapsed}");
            Console.WriteLine();
        }
    }
}
