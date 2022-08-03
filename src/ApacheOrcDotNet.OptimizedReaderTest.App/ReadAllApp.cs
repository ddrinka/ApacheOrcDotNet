using ApacheOrcDotNet.OptimizedReader;
using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using System;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReaderTest.App
{
    public class ReadAllApp
    {
        private readonly string _orcFileUri;
        private readonly IByteRangeProviderFactory _byteRangeProviderFactory;

        public ReadAllApp(string orcFileUri, IByteRangeProviderFactory byteRangeProviderFactory)
        {
            _orcFileUri = orcFileUri;
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

            // Read
            var totalCount = 0;

            for (var stripeId = 0; stripeId < reader.GetNumberOfStripes(); stripeId++)
            {
                for (var rowEntryIndex = 0; rowEntryIndex < reader.GetNumberOfRowGroupEntries(stripeId, timeColumn.Id); rowEntryIndex++)
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

                    for (int idx = 0; idx < reader.NumValuesLoaded; idx++)
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

                        Console.WriteLine($"" +
                            $"{source}," +
                            $"{symbol}," +
                            $"{time.Value.ToString(CultureInfo.InvariantCulture).PadRight(15, '0')}," +
                            $"{size}" +
                            $"     " +
                            $"{(date.HasValue ? date.Value.ToString("MM/dd/yyyy", CultureInfo.InvariantCulture) : string.Empty)}," +
                            $"{dobl}," +
                            $"{sing}," +
                            $"{(timeStamp.HasValue ? timeStamp.Value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) : string.Empty)}," +
                            $"{(binary != null ? Encoding.ASCII.GetString(binary) : string.Empty)}," +
                            $"{tinyInt}," +
                            $"{boolean}" +
                            $""
                        );
                    }
                }
            }

            watch.Stop();
            Console.WriteLine();
            Console.WriteLine($"Read {totalCount} rows.");
            Console.WriteLine($"Read execution time: {watch.Elapsed:mm':'ss':'fff}");
        }
    }
}
