using System;

namespace ApacheOrcDotNet.OptimizedReaderTest.App
{
    public class Item
    {
        public string StringDictionaryV2 { get; set; }
        public string StringDirectV2 { get; set; }
        public decimal? Decimal { get; set; }
        public long? Integer { get; set; }
        public DateTime? Date { get; set; }
        public double? Double { get; set; }
        public float? Float { get; set; }
        public DateTime? TimeStamp { get; set; }
        public byte[] Binary { get; set; }
        public byte? Byte { get; set; }
        public bool? Boolean { get; set; }
    }
}
