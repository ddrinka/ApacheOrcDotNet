using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;

namespace ApacheOrcDotNet.OptimizedReaderTest.App
{
    public class ReadAllOldApp
    {
        private readonly Uri _orcFileUri;

        public ReadAllOldApp(string orcFileUri)
        {
            if (!Uri.TryCreate(orcFileUri, UriKind.Absolute, out var uri))
                throw new InvalidOperationException($"Invalid file:// URI.");

            if (uri.Scheme != "file")
                throw new InvalidOperationException($"Only file:// URIs are supported by this app.");

            _orcFileUri = uri;
        }

        public void Run()
        {
            //
            var totalCount = 0;
            var outputData = true;
            var topwatch = new Stopwatch();
            var fileStream = File.OpenRead(_orcFileUri.LocalPath);
            var orcReader = new OrcReader(typeof(Item), fileStream);

            Console.WriteLine("Read all values with OLD reader.");
            Console.WriteLine();

            topwatch.Start();

            foreach (Item item in orcReader.Read())
            {
                totalCount++;

                if (outputData)
                {
                    Console.WriteLine($"" +
                        $"{item.Source}," +
                        $"{item.Symbol}," +
                        $"{(item.Time.HasValue ? item.Time.Value.ToString(CultureInfo.InvariantCulture).PadRight(15, '0') : string.Empty)}," +
                        $"{item.Size}" +
                        $"     " +
                        $"{(item.Date.HasValue ? item.Date.Value.ToString("MM/dd/yyyy", CultureInfo.InvariantCulture) : string.Empty)}," +
                        $"{item.Double}," +
                        $"{item.Float}," +
                        $"{(item.TimeStamp.HasValue ? item.TimeStamp.Value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) : string.Empty)}," +
                        $"{(item.Binary != null ? System.Text.Encoding.ASCII.GetString(item.Binary) : string.Empty)}," +
                        $"{item.Byte}," +
                        $"{item.Boolean}" +
                        $""
                    );
                }
                else
                {
                    if (totalCount % 10_000 == 0)
                        Console.Write(".");
                }
            }

            topwatch.Stop();
            Console.WriteLine();

            if (!outputData)
                Console.WriteLine();

            Console.WriteLine($"Read {totalCount} rows.");
            Console.WriteLine($"Read execution time: {topwatch.Elapsed:mm':'ss':'fff}");
        }
    }
}
