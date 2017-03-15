using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Test.App
{
    public class Program
    {
        public static void Main(string[] args)
        {
			var rand = new Random(123);
			var testElements = new List<TestClass>();
			for (int i = 0; i < 80000; i++)
			{
				var random = rand.Next();
				var set = i / 10000;
				var randomInRange = (random % 10000) + set * 10000 - 40000;
				var element = new TestClass
				{
					Random = random,
					RandomInRange = randomInRange,
					Incrementing = i,
					SetNumber = set,
					Double = (double)i / (set+1),
					Float = (float)i / (set+1),
					Dec = i / (decimal)Math.Pow(10, set),
					Str = $"Random={random}, RandomInRange={randomInRange}, Incrementing={i}, SetNumber={set}"
				};
				testElements.Add(element);
			}

			using (var fileStream = new FileStream("test.orc", FileMode.Create, FileAccess.Write))
			using (var writer = new OrcWriter<TestClass>(fileStream, new WriterConfiguration())) //Use the default configuration
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
		public double Double { get; set; }
		public float Float { get; set; }
		public decimal Dec { get; set; }
		public string Str { get; set; }
	}
}
