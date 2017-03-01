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
			var random = new Random(123);
			var testElements = new List<TestClass>();
			for (int i = 0; i < 70000; i++)
				testElements.Add(new TestClass { Column1 = random.Next() });

			using (var fileStream = new FileStream("test.orc", FileMode.Create, FileAccess.Write))
			using (var writer = new OrcWriter<TestClass>(fileStream, new WriterConfiguration())) //Use the default configuration
			{
				writer.AddRows(testElements);
			}
		}
	}

	class TestClass
	{
		public int Column1 { get; set; }
	}
}
