using ApacheOrcDotNet.FluentSerialization;
using System;
using System.Collections.Generic;
using System.IO;

namespace ApacheOrcDotNet.Test.App {
    public class Program
    {
        public static void Main(string[] args)
        {
			var baseTime = new DateTime(2017, 3, 16, 0, 0, 0, DateTimeKind.Utc);
			var rand = new Random(123);
			var testElements = new List<TestClass>();
			for (int i = 0; i < 80000; i++)
			{
				var random = rand.Next();
				var set = i / 10000;
				var randomInRange = (random % 10000) + set * 10000 - 40000;
				var dec = (DateTime.Now - DateTime.Today).Ticks / (decimal)TimeSpan.TicksPerSecond;
				var timestamp = baseTime.AddTicks(random);
				var element = new TestClass
				{
					Random = random,
					RandomInRange = randomInRange,
					Incrementing = i,
					SetNumber = set,
					Double = (double)i / (set + 1),
					Float = (float)i / (set + 1),
					Dec = dec,
					Timestamp = timestamp,
					Str = $"Random={random}, RandomInRange={randomInRange}, Incrementing={i}, SetNumber={set}, Dec={dec}, Timestamp={timestamp:MM/dd/yyyy hh:mm:ss.fffffff}",
					DictionaryStr = $"SetNumber={set}"
				};
				testElements.Add(element);
			}

			var serializationConfiguration = new SerializationConfiguration()
					.ConfigureType<TestClass>()
						.ConfigureProperty(x => x.Dec, x => { x.DecimalPrecision = 14; x.DecimalScale = 9; })
						.Build();

			using (var fileStream = new FileStream("test.orc", FileMode.Create, FileAccess.Write))
			using (var writer = new OrcWriter<TestClass>(fileStream, new WriterConfiguration(), serializationConfiguration)) //Use the default configuration
			{
				writer.AddRows(testElements);
			}
		}
	}

	class TestClass
	{
		public int Random { get; set; }
		public int RandomInRange { get; set; }
		public int Incrementing { get; set; }
		public int SetNumber { get; set; }
		public int? AllNulls { get; set; }
		public double Double { get; set; }
		public float Float { get; set; }
		public decimal Dec { get; set; }
		public decimal? AllNullsDec { get; set; }
		public DateTime Timestamp { get; set; }
		public string Str { get; set; }
		public string DictionaryStr { get; set; }
	}
}
