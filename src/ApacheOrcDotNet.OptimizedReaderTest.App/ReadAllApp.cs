using ApacheOrcDotNet.OptimizedReader;
using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using System;
using System.Diagnostics;
using System.Globalization;
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

            Console.WriteLine("Read all values with NEW reader.");
            Console.WriteLine();

            watch.Start();

            // Columns
            var stringDictionaryV2Column = reader.GetColumn("stringDictionaryV2");
            var stringDirectV2Column = reader.GetColumn("stringDirectV2");
            var decimalColumn = reader.GetColumn("decimal");
            var integerColumn = reader.GetColumn("integer");
            var dateColumn = reader.GetColumn("date");
            var doubleColumn = reader.GetColumn("double");
            var floatColumn = reader.GetColumn("float");
            var timeStampColumn = reader.GetColumn("timeStamp");
            var binaryColumn = reader.GetColumn("binary");
            var byteColumn = reader.GetColumn("byte");
            var booleanColumn = reader.GetColumn("boolean");

            // Buffers
            var stringDictionaryV2ColumnBuffer = reader.CreateStringColumnBuffer(stringDictionaryV2Column);
            var stringDirectV2ColumnBuffer = reader.CreateStringColumnBuffer(stringDirectV2Column);
            var decimalColumnBuffer = reader.CreateDecimalColumnBuffer(decimalColumn);
            var integerColumnBuffer = reader.CreateIntegerColumnBuffer(integerColumn);
            var dateColumnBuffer = reader.CreateDateColumnBuffer(dateColumn);
            var doubleColumnBuffer = reader.CreateDoubleColumnBuffer(doubleColumn);
            var floatColumnBuffer = reader.CreateFloatColumnBuffer(floatColumn);
            var timeStampColumnBuffer = reader.CreateTimestampColumnBuffer(timeStampColumn);
            var binaryColumnBuffer = reader.CreateBinaryColumnBuffer(binaryColumn);
            var byteColumnBuffer = reader.CreateByteColumnBuffer(byteColumn);
            var booleanColumnBuffer = reader.CreateBooleanColumnReader(booleanColumn);

            // Read
            var totalCount = 0;
            var outputData = true;

            for (var stripeId = 0; stripeId < reader.GetNumberOfStripes(); stripeId++)
            {
                var numRowEntryIndexes = reader.GetNumberOfRowGroupEntries(stripeId, decimalColumn.Id);

                for (var rowEntryIndex = 0; rowEntryIndex < numRowEntryIndexes; rowEntryIndex++)
                {
                    await Task.WhenAll(
                        reader.LoadDataAsync(stripeId, rowEntryIndex, stringDictionaryV2ColumnBuffer),
                        reader.LoadDataAsync(stripeId, rowEntryIndex, stringDirectV2ColumnBuffer),
                        reader.LoadDataAsync(stripeId, rowEntryIndex, decimalColumnBuffer),
                        reader.LoadDataAsync(stripeId, rowEntryIndex, integerColumnBuffer),
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

                        var stringDictionaryV2 = stringDictionaryV2ColumnBuffer.Values[idx];
                        var stringDirectV2 = stringDirectV2ColumnBuffer.Values[idx];
                        var @decimal = decimalColumnBuffer.Values[idx];
                        var integer = integerColumnBuffer.Values[idx];
                        var date = dateColumnBuffer.Values[idx];
                        var dobl = doubleColumnBuffer.Values[idx];
                        var sing = floatColumnBuffer.Values[idx];
                        var timeStamp = timeStampColumnBuffer.Values[idx];
                        var binary = binaryColumnBuffer.Values[idx];
                        var tinyInt = byteColumnBuffer.Values[idx];
                        var boolean = booleanColumnBuffer.Values[idx];

                        if (outputData)
                        {
                            Console.WriteLine($"" +
                                $"{stringDictionaryV2}," +
                                $"{stringDirectV2}," +
                                $"{(@decimal.HasValue ? @decimal.Value.ToString(CultureInfo.InvariantCulture).PadRight(15, '0') : string.Empty)}," +
                                $"{integer}" +
                                $"     " +
                                $"{(date.HasValue ? date.Value.ToString("MM/dd/yyyy", CultureInfo.InvariantCulture) : string.Empty)}," +
                                $"{dobl}," +
                                $"{sing}," +
                                $"{(timeStamp.HasValue ? timeStamp.Value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) : string.Empty)}," +
                                $"{(binary != null ? System.Text.Encoding.ASCII.GetString(binary) : string.Empty)}," +
                                $"{tinyInt}," +
                                $"{boolean}" +
                                $""
                            );
                        }
                        else
                        {
                            if (totalCount % 10_000 == 0)
                                Console.Write(".");
                        }
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
