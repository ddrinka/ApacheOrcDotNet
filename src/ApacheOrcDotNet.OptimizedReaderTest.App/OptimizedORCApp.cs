using ApacheOrcDotNet.OptimizedReader;
using System;

namespace ApacheOrcDotNet.OptimizedReaderTest.App
{
    public class OptimizedORCAppConfituration
    {
        public DateTime Date { get; set; }
        public string Source { get; set; }
        public string Symbol { get; set; }
        public TimeSpan BeginTime { get; set; }
        public TimeSpan EndTime { get; set; }
    }

    public class OptimizedORCApp
    {
        private readonly OptimizedORCAppConfituration _confituration;
        private readonly IByteRangeProviderFactory _byteRangeProviderFactory;

        public OptimizedORCApp(OptimizedORCAppConfituration confituration, IByteRangeProviderFactory byteRangeProviderFactory)
        {
            _confituration = confituration;
            _byteRangeProviderFactory = byteRangeProviderFactory;
        }

        public void Run()
        {
            var byteRangeProvider = _byteRangeProviderFactory.Create(_confituration.Source);
            var optimizedOrcReader = new OptimizedReader.OrcReader(new OrcReaderConfiguration(), byteRangeProvider);

            var rowGroupIndex = optimizedOrcReader.ReadRowGroupIndex(columnId: 0, stripeId: 0);
        }
    }
}
