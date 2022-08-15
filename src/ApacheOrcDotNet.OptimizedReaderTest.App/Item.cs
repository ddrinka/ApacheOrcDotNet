using System;

namespace ApacheOrcDotNet.OptimizedReaderTest.App
{
    public class Item
    {
        public string Source { get; set; }
        public string Symbol { get; set; }
        public decimal? Time { get; set; }
        public long? Size { get; set; }
        public DateTime? Date { get; set; }
        public double? Double { get; set; }
        public float? Float { get; set; }
        public DateTime? TimeStamp { get; set; }
        public byte[] Binary { get; set; }
        public byte? Byte { get; set; }
        public bool? Boolean { get; set; }
    }
}
