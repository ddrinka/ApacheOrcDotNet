using ApacheOrcDotNet.OptimizedReader;
using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using System;
using System.Diagnostics;

namespace ApacheOrcDotNet.OptimizedReaderTest.App
{
    public class SampleDataSourceApp
    {
        private readonly string _orcFileUri;
        private readonly Configs _configuration;
        private readonly IByteRangeProviderFactory _byteRangeProviderFactory;

        public SampleDataSourceApp(string orcFileUri, Configs configuration, IByteRangeProviderFactory byteRangeProviderFactory)
        {
            _orcFileUri = orcFileUri;
            _configuration = configuration;
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

            var timeRanges = new (TimeSpan startTime, TimeSpan endTime)[] {
                (new TimeSpan(09, 43, 20), new TimeSpan(09, 43, 21)), // 35000, 35001
                (new TimeSpan(09, 44, 37), new TimeSpan(09, 44, 38)), // 35077, 35078
                (new TimeSpan(12, 00, 00), new TimeSpan(13, 00, 00)), // 43200, 46800
                (new TimeSpan(14, 00, 00), new TimeSpan(15, 00, 00)), // 50400, 54000
                (new TimeSpan(09, 43, 20), new TimeSpan(11, 43, 20))  // 35000, 42200
            };

            var dataSource = new SampleDataSource(reader, _configuration.Vendor, _configuration.Product);

            foreach (var (sTime, eTime) in timeRanges)
            {
                var timeRangeReader = dataSource.CreateTimeRangeReader(sTime, eTime);

                var approxRowCount = timeRangeReader.ApproxRowCount;

                var times = new decimal?[approxRowCount];
                var sales = new long?[approxRowCount];
                var numRows = 0;

                while (true)
                {
                    var rowsRead = timeRangeReader.ReadBatch(
                        times.AsSpan()[numRows..],
                        sales.AsSpan()[numRows..]
                    );

                    if (rowsRead == 0)
                        break;

                    Console.Write(".");

                    numRows += rowsRead;
                }

                Console.WriteLine();
                Console.WriteLine($"Read {numRows} rows of data");
            }

            watch.Stop();
            Console.WriteLine();
            Console.WriteLine($"Read execution time: {watch.Elapsed:mm':'ss':'fff}");
        }
    }
}
