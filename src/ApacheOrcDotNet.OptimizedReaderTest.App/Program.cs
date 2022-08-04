using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using Microsoft.Extensions.Configuration;
using System;
using System.Diagnostics;
using System.Globalization;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReaderTest.App
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: true)
                .AddEnvironmentVariables("apacheorc_app_")
                .AddCommandLine(args)
                .Build()
            ;

            var uri = config.GetValue("uri", string.Empty);
            var source = config.GetValue("source", string.Empty);
            var symbol = config.GetValue("symbol", string.Empty);
            var beginTime = config.GetValue("beginTime", "00:00:00");
            var endTime = config.GetValue("endTime", "23:45:00");

            var isValidBeginTime = TimeSpan.TryParse(beginTime, CultureInfo.InvariantCulture, out var parsedBeginTime);
            var isValidEndTime = TimeSpan.TryParse(endTime, CultureInfo.InvariantCulture, out var parsedEndTime);

            if (uri.Length ==0 || source.Length == 0 || symbol.Length == 0 || !isValidBeginTime || !isValidEndTime || (parsedEndTime < parsedBeginTime))
            {
                Console.WriteLine("Usage: --uri orcFileUri --source sourceName --symbol symbolName --beginTime hh:mm:ss --endTime hh:mm:ss");
                Console.WriteLine();
                Console.WriteLine("Examples:");
                Console.WriteLine(@"   dotnet run --uri file://c:/path/to/testFile.orc --source CTSPillarNetworkB --symbol SPY --beginTime 09:43:20 --endTime 09:43:21");
                Console.WriteLine(@"   dotnet run --uri https://s3.amazonaws.com/some/path/testFile.orc --source CTSPillarNetworkB --symbol SPY --beginTime 09:43:20 --endTime 09:43:21");
                Console.WriteLine();
                Console.WriteLine(@"   You can use files under ApacheOrcDotNet.OptimizedReader.Test/Data to test the readers:");
                Console.WriteLine(@"      - optimized_reader_test_file.orc");
                Console.WriteLine(@"      - optimized_reader_test_file_no_nulls.orc");
                Console.WriteLine();
                Environment.Exit(-1);
            }

            Console.WriteLine("Running.. CTRL+C to exit.");
            Console.WriteLine();
            Console.WriteLine($"Pid: {Environment.ProcessId}");
            Console.WriteLine($"source: '{source}'");
            Console.WriteLine($"symbol: '{symbol}'");
            Console.WriteLine($"beginTime: '{beginTime}'");
            Console.WriteLine($"endTime: '{endTime}'");
            Console.WriteLine();

            var configs = new Configs
            {
                Source = source,
                Symbol = symbol,
                BeginTime = parsedBeginTime,
                EndTime = parsedEndTime
            };

            var fileByteRangeProviderFactory = new ByteRangeProviderFactory();
            var stopWatch = new Stopwatch();

            stopWatch.Start();

            // Sample app 1
            await (new ReadAllApp(uri, fileByteRangeProviderFactory)).Run();

            // Sample app 2
            //await Task.Delay(0);
            //(new ReadAllOldApp(uri)).Run();

            // Sample app 3
            //await (new ReadFilteredApp(uri, configs, fileByteRangeProviderFactory)).Run();

            //// Sample app 4
            //// This requires a test file with a sorce,symbol,time,price and size fields.
            //// (Or the test class below can be updated to use different fields)
            //await Task.Delay(0);
            //(new TradeDataSourceApp(uri, configs, fileByteRangeProviderFactory)).Run();

            stopWatch.Stop();

            Console.WriteLine($"Total execution time: {stopWatch.Elapsed:mm':'ss':'fff}");
            Console.WriteLine();
        }
    }
}
