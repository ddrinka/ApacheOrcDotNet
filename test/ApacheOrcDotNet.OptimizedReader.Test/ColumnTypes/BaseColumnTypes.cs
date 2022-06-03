using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Test.TestHelpers;
using System.Collections.Generic;
using System.Globalization;
using System.IO;

namespace ApacheOrcDotNet.OptimizedReader.Test.ColumnTypes
{
    public abstract class _BaseColumnTypeWithNulls : BaseColumnTypes
    {
        public _BaseColumnTypeWithNulls() : base("optimized_reader_test_file")
        {
        }
    }

    public abstract class _BaseColumnTypeWithoutNulls : BaseColumnTypes
    {
        public _BaseColumnTypeWithoutNulls() : base("optimized_reader_test_file_no_nulls")
        {
        }
    }

    public abstract class BaseColumnTypes
    {
        private protected readonly CultureInfo _enUSCulture = CultureInfo.GetCultureInfo("en-US");
        private protected readonly (List<string> sources, List<string> symbols, List<string> times, List<string> timesAsDouble, List<string> sizes, List<string> dates, List<string> doubles, List<string> floats, List<string> timestamps, List<string> binaries, List<string> bytes, List<string> booleans) _expectedValues
            = (new List<string>(), new List<string>(), new List<string>(), new List<string>(), new List<string>(), new List<string>(), new List<string>(), new List<string>(), new List<string>(), new List<string>(), new List<string>(), new List<string>());
        private protected readonly IByteRangeProvider _byteRangeProvider;
        private readonly DataFileHelper _expectedValuesFile;

        public BaseColumnTypes(string fileName)
        {
            _byteRangeProvider = new TestByteRangeProviderParallel($"{fileName}.orc");
            _expectedValuesFile = new DataFileHelper(typeof(TestByteRangeProvider), $"{fileName}.csv");

            var expectedDataStream = _expectedValuesFile.GetStream();
            using (StreamReader reader = new StreamReader(expectedDataStream))
            {
                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    var expectedValues = line.Split(',');

                    _expectedValues.sources.Add(string.IsNullOrWhiteSpace(expectedValues[0]) ? null : expectedValues[0]);
                    _expectedValues.symbols.Add(string.IsNullOrWhiteSpace(expectedValues[1]) ? null : expectedValues[1]);
                    _expectedValues.times.Add(string.IsNullOrWhiteSpace(expectedValues[2]) ? null : expectedValues[2]);
                    _expectedValues.timesAsDouble.Add(string.IsNullOrWhiteSpace(expectedValues[2]) ? null : expectedValues[2]);
                    _expectedValues.sizes.Add(string.IsNullOrWhiteSpace(expectedValues[3]) ? null : expectedValues[3]);
                    _expectedValues.dates.Add(string.IsNullOrWhiteSpace(expectedValues[4]) ? null : expectedValues[4]);
                    _expectedValues.doubles.Add(string.IsNullOrWhiteSpace(expectedValues[5]) ? null : expectedValues[5]);
                    _expectedValues.floats.Add(string.IsNullOrWhiteSpace(expectedValues[6]) ? null : expectedValues[6]);
                    _expectedValues.timestamps.Add(string.IsNullOrWhiteSpace(expectedValues[7]) ? null : expectedValues[7]);
                    _expectedValues.binaries.Add(string.IsNullOrWhiteSpace(expectedValues[8]) ? null : expectedValues[8]);
                    _expectedValues.bytes.Add(string.IsNullOrWhiteSpace(expectedValues[9]) ? null : expectedValues[9]);
                    _expectedValues.booleans.Add(string.IsNullOrWhiteSpace(expectedValues[10]) ? null : expectedValues[10]);
                }
            }
        }

        ~BaseColumnTypes()
        {
            _byteRangeProvider.Dispose();
        }
    }
}
