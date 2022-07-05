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
        private protected readonly CultureInfo _invariantCulture = CultureInfo.InvariantCulture;
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

                    ExpectedSources.Add(string.IsNullOrWhiteSpace(expectedValues[0]) ? null : expectedValues[0]);
                    ExpectedSymbols.Add(string.IsNullOrWhiteSpace(expectedValues[1]) ? null : expectedValues[1]);
                    ExpectedTimes.Add(string.IsNullOrWhiteSpace(expectedValues[2]) ? null : expectedValues[2]);
                    ExpectedTimesAsDouble.Add(string.IsNullOrWhiteSpace(expectedValues[2]) ? null : expectedValues[2]);
                    ExpectedSizes.Add(string.IsNullOrWhiteSpace(expectedValues[3]) ? null : expectedValues[3]);
                    ExpectedDates.Add(string.IsNullOrWhiteSpace(expectedValues[4]) ? null : expectedValues[4]);
                    ExpectedDoubles.Add(string.IsNullOrWhiteSpace(expectedValues[5]) ? null : expectedValues[5]);
                    ExpectedFloats.Add(string.IsNullOrWhiteSpace(expectedValues[6]) ? null : expectedValues[6]);
                    ExpectedTimestamps.Add(string.IsNullOrWhiteSpace(expectedValues[7]) ? null : expectedValues[7]);
                    ExpectedBinaries.Add(string.IsNullOrWhiteSpace(expectedValues[8]) ? null : expectedValues[8]);
                    ExpectedBytes.Add(string.IsNullOrWhiteSpace(expectedValues[9]) ? null : expectedValues[9]);
                    ExpectedBooleans.Add(string.IsNullOrWhiteSpace(expectedValues[10]) ? null : expectedValues[10]);
                }
            }
        }

        protected List<string> ExpectedSources { get; private set; } = new();
        protected List<string> ExpectedSymbols { get; private set; } = new();
        protected List<string> ExpectedTimes { get; private set; } = new();
        protected List<string> ExpectedTimesAsDouble { get; private set; } = new();
        protected List<string> ExpectedSizes { get; private set; } = new();
        protected List<string> ExpectedDates { get; private set; } = new();
        protected List<string> ExpectedDoubles { get; private set; } = new();
        protected List<string> ExpectedFloats { get; private set; } = new();
        protected List<string> ExpectedTimestamps { get; private set; } = new();
        protected List<string> ExpectedBinaries { get; private set; } = new();
        protected List<string> ExpectedBytes { get; private set; } = new();
        protected List<string> ExpectedBooleans { get; private set; } = new();

        ~BaseColumnTypes()
        {
            _byteRangeProvider.Dispose();
        }
    }
}
