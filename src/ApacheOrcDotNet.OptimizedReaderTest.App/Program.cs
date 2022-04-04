using ApacheOrcDotNet.OptimizedReader;
using Microsoft.Extensions.Configuration;
using System;
using System.Diagnostics;
using System.Globalization;

namespace ApacheOrcDotNet.OptimizedReaderTest.App
{
    internal class Program
    {
        static void Main(string[] args)
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

            var date = config.GetValue("date", DateTime.Now.ToString("d"));
            var source = config.GetValue("source", string.Empty);
            var symbol = config.GetValue("symbol", string.Empty);
            var beginTime = config.GetValue("beginTime", "00:00:00");
            var endTime = config.GetValue("endTime", "23:45:00");

            var isValidDate = DateTime.TryParse(date, out var parsedDate);
            var isValidBeginTime = TimeSpan.TryParse(beginTime, out var parsedBeginTime);
            var isValidEndTime = TimeSpan.TryParse(endTime, out var parsedEndTime);

            if (!isValidDate || source.Length == 0 || symbol.Length == 0 || !isValidBeginTime || !isValidEndTime || (parsedEndTime < parsedBeginTime))
            {
                Console.WriteLine("Usage: --date m/d/yyyy --source sourceName --symbol symbolName --beginTime hh:mm:ss --endTime hh:mm:ss");
                Environment.Exit(-1);
            }

            Console.WriteLine("Running.. CTRL+C to exit.");
            Console.WriteLine();
            Console.WriteLine($"Pid: {Environment.ProcessId}");
            Console.WriteLine($"date: '{date}'");
            Console.WriteLine($"source: '{source}'");
            Console.WriteLine($"symbol: '{symbol}'");
            Console.WriteLine($"beginTime: '{beginTime}'");
            Console.WriteLine($"endTime: '{endTime}'");

            var configs = new OptimizedORCAppConfituration
            {
                Date = parsedDate,
                Source = source,
                Symbol = symbol,
                BeginTime = parsedBeginTime,
                EndTime = parsedEndTime
            };

            var fileByteRangeProviderFactory = new FileByteRangeProviderFactory();
            var optimizedORCApp = new OptimizedORCApp(configs, fileByteRangeProviderFactory);
            var stopWatch = new Stopwatch();

            stopWatch.Start();
            optimizedORCApp.Run();
            stopWatch.Stop();

            Console.WriteLine();
            Console.WriteLine($"Total execution time: {stopWatch.Elapsed}");
            Console.WriteLine();
        }
    }
}
