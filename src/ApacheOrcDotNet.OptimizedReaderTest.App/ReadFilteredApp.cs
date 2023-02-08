using ApacheOrcDotNet.OptimizedReader;
using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using System;
using System.Diagnostics;
using System.Globalization;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReaderTest.App
{
    public class ReadFilteredApp
    {
        private readonly string _orcFileUri;
        private readonly Configs _configuration;
        private readonly IByteRangeProviderFactory _byteRangeProviderFactory;

        public ReadFilteredApp(string orcFileUri, Configs confituration, IByteRangeProviderFactory byteRangeProviderFactory)
        {
            _orcFileUri = orcFileUri;
            _configuration = confituration;
            _byteRangeProviderFactory = byteRangeProviderFactory;
        }

        public async Task Run()
        {
            //
            var watch = new Stopwatch();
            var configs = new OrcReaderConfiguration();
            var rangeProvider = _byteRangeProviderFactory.Create(_orcFileUri);
            var reader = new OptimizedReader.OrcReader(configs, rangeProvider);

            Console.WriteLine("Read FILTERED with NEW reader.");
            Console.WriteLine();

            watch.Start();

            // Args
            var lookupVendor = _configuration.Vendor;
            var lookupProduct = _configuration.Product;
            var beginTime = _configuration.BeginTime;
            var endTime = _configuration.EndTime;

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

            // Filters
            var vendorFilterValues = FilterValues.CreateFromString(min: lookupVendor, max: lookupVendor);
            var productFilterValues = FilterValues.CreateFromString(min: lookupProduct, max: lookupProduct);
            var timeFilterValues = FilterValues.CreateFromTime(min: beginTime, max: endTime);

            //
            var stripeIds = reader.FilterStripes(stringDictionaryV2Column, vendorFilterValues);
            stripeIds = reader.FilterStripes(stripeIds, stringDirectV2Column, productFilterValues);
            stripeIds = reader.FilterStripes(stripeIds, decimalColumn, timeFilterValues);

            foreach (var stripeId in stripeIds)
            {
                //
                var rowGroupIndexes = reader.FilterRowGroups(stripeId, stringDictionaryV2Column, vendorFilterValues);
                rowGroupIndexes = reader.FilterRowGroups(rowGroupIndexes, stripeId, stringDirectV2Column, productFilterValues);
                rowGroupIndexes = reader.FilterRowGroups(rowGroupIndexes, stripeId, decimalColumn, timeFilterValues);

                foreach (var rowEntryIndex in rowGroupIndexes)
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

                        if (stringDictionaryV2 == lookupVendor && stringDirectV2 == lookupProduct && @decimal >= (decimal)beginTime.TotalSeconds && @decimal <= (decimal)endTime.TotalSeconds)
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
                    }
                }
            }

            watch.Stop();
            Console.WriteLine();
            Console.WriteLine($"Read execution time: {watch.Elapsed:mm':'ss':'fff}");
        }
    }
}
