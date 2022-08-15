using ApacheOrcDotNet.OptimizedReader;
using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReaderTest.App
{
    public class ReadCompareApp
    {
        private readonly Uri _orcFileUri1;
        private readonly Uri _orcFileUri2;
        private readonly IByteRangeProviderFactory _byteRangeProviderFactory;

        public ReadCompareApp(string orcFileUri1, string orcFileUri2, IByteRangeProviderFactory byteRangeProviderFactory)
        {
            var parseUri1 = Uri.TryCreate(orcFileUri1, UriKind.Absolute, out var uri1);
            var parseUri2 = Uri.TryCreate(orcFileUri2, UriKind.Absolute, out var uri2);

            if (!parseUri1 || ! parseUri2)
                throw new InvalidOperationException($"Invalid file:// URI.");

            if (uri1.Scheme != "file" || uri2.Scheme != "file")
                throw new InvalidOperationException($"Only file:// URIs are supported by this app.");

            _orcFileUri1 = uri1;
            _orcFileUri2 = uri2;

            _byteRangeProviderFactory = byteRangeProviderFactory;
        }

        public async Task Run()
        {
            //
            var watch = new Stopwatch();
            var configs = new OrcReaderConfiguration();
            var fileStream = File.OpenRead(_orcFileUri1.LocalPath);
            var rangeProvider = _byteRangeProviderFactory.Create(_orcFileUri2.LocalPath);
            var orcReaderNew = new OptimizedReader.OrcReader(configs, rangeProvider);
            var orcReaderOld = new OrcReader(typeof(Item), fileStream);

            Console.WriteLine("Comparing values from old and new reader.");
            Console.WriteLine();

            watch.Start();

            // Columns
            var sourceColumn = orcReaderNew.GetColumn("source");
            var symbolColumn = orcReaderNew.GetColumn("symbol");
            var timeColumn = orcReaderNew.GetColumn("time");
            var sizeColumn = orcReaderNew.GetColumn("size");
            var dateColumn = orcReaderNew.GetColumn("date");
            var doubleColumn = orcReaderNew.GetColumn("double");
            var floatColumn = orcReaderNew.GetColumn("float");
            var timeStampColumn = orcReaderNew.GetColumn("timeStamp");
            var binaryColumn = orcReaderNew.GetColumn("binary");
            var byteColumn = orcReaderNew.GetColumn("byte");
            var booleanColumn = orcReaderNew.GetColumn("boolean");

            // Buffers
            var sourceColumnBuffer = orcReaderNew.CreateStringColumnBuffer(sourceColumn);
            var symbolColumnBuffer = orcReaderNew.CreateStringColumnBuffer(symbolColumn);
            var timeColumnBuffer = orcReaderNew.CreateDecimalColumnBuffer(timeColumn);
            var sizeColumnBuffer = orcReaderNew.CreateIntegerColumnBuffer(sizeColumn);
            var dateColumnBuffer = orcReaderNew.CreateDateColumnBuffer(dateColumn);
            var doubleColumnBuffer = orcReaderNew.CreateDoubleColumnBuffer(doubleColumn);
            var floatColumnBuffer = orcReaderNew.CreateFloatColumnBuffer(floatColumn);
            var timeStampColumnBuffer = orcReaderNew.CreateTimestampColumnBuffer(timeStampColumn);
            var binaryColumnBuffer = orcReaderNew.CreateBinaryColumnBuffer(binaryColumn);
            var byteColumnBuffer = orcReaderNew.CreateByteColumnBuffer(byteColumn);
            var booleanColumnBuffer = orcReaderNew.CreateBooleanColumnReader(booleanColumn);

            // Read
            var totalCount = 0;
            var outputData = false;

            var oldReaderItemsEnumerator = orcReaderOld.Read().GetEnumerator();

            for (var stripeId = 0; stripeId < orcReaderNew.GetNumberOfStripes(); stripeId++)
            {
                var numRowEntryIndexes = orcReaderNew.GetNumberOfRowGroupEntries(stripeId, timeColumn.Id);

                for (var rowEntryIndex = 0; rowEntryIndex < numRowEntryIndexes; rowEntryIndex++)
                {
                    await Task.WhenAll(
                        orcReaderNew.LoadDataAsync(stripeId, rowEntryIndex, sourceColumnBuffer),
                        orcReaderNew.LoadDataAsync(stripeId, rowEntryIndex, symbolColumnBuffer),
                        orcReaderNew.LoadDataAsync(stripeId, rowEntryIndex, timeColumnBuffer),
                        orcReaderNew.LoadDataAsync(stripeId, rowEntryIndex, sizeColumnBuffer),
                        orcReaderNew.LoadDataAsync(stripeId, rowEntryIndex, dateColumnBuffer),
                        orcReaderNew.LoadDataAsync(stripeId, rowEntryIndex, doubleColumnBuffer),
                        orcReaderNew.LoadDataAsync(stripeId, rowEntryIndex, floatColumnBuffer),
                        orcReaderNew.LoadDataAsync(stripeId, rowEntryIndex, timeStampColumnBuffer),
                        orcReaderNew.LoadDataAsync(stripeId, rowEntryIndex, binaryColumnBuffer),
                        orcReaderNew.LoadDataAsync(stripeId, rowEntryIndex, byteColumnBuffer),
                        orcReaderNew.LoadDataAsync(stripeId, rowEntryIndex, booleanColumnBuffer)
                    );

                    for (int idx = 0; idx < orcReaderNew.NumValuesLoaded; idx++)
                    {
                        totalCount++;

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

                        if (!oldReaderItemsEnumerator.MoveNext())
                        {
                            Console.Write($" (Skipping {totalCount})");
                            continue;
                        }

                        var item = (Item)oldReaderItemsEnumerator.Current;

                        if (source != item.Source || symbol != item.Symbol || time != item.Time || size != item.Size)
                            throw new InvalidDataException($"{source},{symbol},{time},{size} != {item.Source},{item.Symbol},{item.Time},{item.Size}");

                        if (date != item.Date || dobl != item.Double || sing != item.Float || timeStamp != item.TimeStamp)
                            throw new InvalidDataException($"{date},{dobl},{sing},{timeStamp} != {item.Date},{item.Double},{item.Float},{item.TimeStamp}");

                        if (!(binary ?? new byte[0]).SequenceEqual(item.Binary ?? new byte[0]) || tinyInt != item.Byte || boolean != item.Boolean)
                            throw new InvalidDataException($"{binary},{tinyInt},{boolean} != {item.Binary},{item.Byte},{item.Boolean}");

                        if (totalCount % 10_000 == 0)
                            Console.Write(".");
                    }
                }
            }

            watch.Stop();
            Console.WriteLine();

            if (!outputData)
                Console.WriteLine();

            Console.WriteLine($"Read {totalCount} rows.");
            Console.WriteLine($"Read execution time: {watch.Elapsed:mm':'ss':'fff}");
        }
    }
}
