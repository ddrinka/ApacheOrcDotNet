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
            var cultureInfo = CultureInfo.GetCultureInfo("en-US");

            CultureInfo.DefaultThreadCurrentUICulture = cultureInfo;
            CultureInfo.DefaultThreadCurrentCulture = cultureInfo;

            var config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: true)
                .AddEnvironmentVariables("apacheorc_app_")
                .AddCommandLine(args)
                .Build()
            ;

            var uri = config.GetValue("uri", string.Empty);
            var date = config.GetValue("date", DateTime.Now.ToString("d"));
            var source = config.GetValue("source", string.Empty);
            var symbol = config.GetValue("symbol", string.Empty);
            var beginTime = config.GetValue("beginTime", "00:00:00");
            var endTime = config.GetValue("endTime", "23:45:00");

            var isValidDate = DateTime.TryParse(date, out var parsedDate);
            var isValidBeginTime = TimeSpan.TryParse(beginTime, out var parsedBeginTime);
            var isValidEndTime = TimeSpan.TryParse(endTime, out var parsedEndTime);

            if (uri.Length ==0 || !isValidDate || source.Length == 0 || symbol.Length == 0 || !isValidBeginTime || !isValidEndTime || (parsedEndTime < parsedBeginTime))
            {
                Console.WriteLine("Usage: --uri orcFileUri --date m/d/yyyy --source sourceName --symbol symbolName --beginTime hh:mm:ss --endTime hh:mm:ss");
                Console.WriteLine();
                Console.WriteLine("Examples:");
                Console.WriteLine(@"   dotnet run --uri file://c:/path/to/testFile.orc --source CTSPillarNetworkB --symbol SPY --beginTime 09:43:20 --endTime 09:43:21");
                Console.WriteLine(@"   dotnet run --uri https://s3.amazonaws.com/some/path/testFile.orc --source CTSPillarNetworkB --symbol SPY --beginTime 09:43:20 --endTime 09:43:21");
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

            var configs = new OptimizedORCAppConfiguration
            {
                Date = parsedDate,
                Source = source,
                Symbol = symbol,
                BeginTime = parsedBeginTime,
                EndTime = parsedEndTime
            };

            var fileByteRangeProviderFactory = new ByteRangeProviderFactory();
            var optimizedORCApp = new OptimizedORCApp(uri, configs, fileByteRangeProviderFactory);
            var stopWatch = new Stopwatch();

            stopWatch.Start();
            await optimizedORCApp.Run();
            stopWatch.Stop();

            Console.WriteLine($"Total execution time: {stopWatch.Elapsed}");
            Console.WriteLine();
        }
    }
}
