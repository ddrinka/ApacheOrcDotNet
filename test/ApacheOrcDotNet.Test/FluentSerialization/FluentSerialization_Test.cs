
/* Unmerged change from project 'ApacheOrcDotNet.Test (net50)'
Before:
using System;
After:
using ApacheOrcDotNet.FluentSerialization;
using System;
*/

/* Unmerged change from project 'ApacheOrcDotNet.Test (net461)'
Before:
using System;
After:
using ApacheOrcDotNet.FluentSerialization;
using System;
*/
using ApacheOrcDotNet.FluentSerialization;
using System
/* Unmerged change from project 'ApacheOrcDotNet.Test (net50)'
Before:
using ApacheOrcDotNet.FluentSerialization;
using Xunit;
After:
using Xunit;
*/

/* Unmerged change from project 'ApacheOrcDotNet.Test (net461)'
Before:
using ApacheOrcDotNet.FluentSerialization;
using Xunit;
After:
using Xunit;
*/
;
using Xunit;

namespace ApacheOrcDotNet.Test.FluentSerialization {
    public class FluentSerialization_Test
    {
		[Fact]
		public void FluentSerialization_IdentifiesBlittableProperties()
		{
			var conf = new SerializationConfiguration();
			conf.ConfigureType<TestType>()
				.ConfigureProperty(x => x.IntColumn, x => x.ExcludeFromSerialization = true)
				.ConfigureProperty(x => x.DecColumn, x => x.ExcludeFromSerialization = true)
				.ConfigureProperty(x => x.TimeColumn, x => x.ExcludeFromSerialization = true)
				.ConfigureProperty(x => x.EnumColumn, x => x.ExcludeFromSerialization = true)
				.ConfigureProperty(x => x.StructColumn, x => x.ExcludeFromSerialization = true)
			;

			var properties = conf.Types[typeof(TestType)].Properties;
			Assert.Contains(typeof(TestType).GetProperty("IntColumn"), properties.Keys);
			Assert.Contains(typeof(TestType).GetProperty("DecColumn"), properties.Keys);
			Assert.Contains(typeof(TestType).GetProperty("TimeColumn"), properties.Keys);
			Assert.Contains(typeof(TestType).GetProperty("EnumColumn"), properties.Keys);
			Assert.Contains(typeof(TestType).GetProperty("StructColumn"), properties.Keys);

			foreach (var propertyConfiguration in properties.Values)
				Assert.True(propertyConfiguration.ExcludeFromSerialization);
		}

		[Fact]
		public void FluentSerialization_IdentifiesNullableProperties()
		{
			var conf = new SerializationConfiguration();
			conf.ConfigureType<TestType>()
				.ConfigureProperty(x => x.NullableIntColumn, x => x.ExcludeFromSerialization = true)
				.ConfigureProperty(x => x.NullableDecColumn, x => x.ExcludeFromSerialization = true)
				.ConfigureProperty(x => x.NullableTimeColumn, x => x.ExcludeFromSerialization = true)
				.ConfigureProperty(x => x.NullableEnumColumn, x => x.ExcludeFromSerialization = true)
			;

			var properties = conf.Types[typeof(TestType)].Properties;
			Assert.Contains(typeof(TestType).GetProperty("NullableIntColumn"), properties.Keys);
			Assert.Contains(typeof(TestType).GetProperty("NullableDecColumn"), properties.Keys);
			Assert.Contains(typeof(TestType).GetProperty("NullableTimeColumn"), properties.Keys);
			Assert.Contains(typeof(TestType).GetProperty("NullableEnumColumn"), properties.Keys);

			foreach (var propertyConfiguration in properties.Values)
				Assert.True(propertyConfiguration.ExcludeFromSerialization);
		}

		[Fact]
		public void FluentSerialization_IdentifiesReferenceProperties()
		{
			var conf = new SerializationConfiguration();
			conf.ConfigureType<TestType>()
				.ConfigureProperty(x => x.StrColumn, x => x.ExcludeFromSerialization = true)
				.ConfigureProperty(x => x.ClassColumn, x => x.ExcludeFromSerialization = true)
			;

			var properties = conf.Types[typeof(TestType)].Properties;
			Assert.Contains(typeof(TestType).GetProperty("StrColumn"), properties.Keys);
			Assert.Contains(typeof(TestType).GetProperty("ClassColumn"), properties.Keys);

			foreach (var propertyConfiguration in properties.Values)
				Assert.True(propertyConfiguration.ExcludeFromSerialization);
		}

		//TODO add tests for inheritance and interface implementations
	}


	enum TestEnum { One, Two }
	class TestType
	{
		public int IntColumn { get; set; }
		public decimal DecColumn { get; set; }
		public DateTime TimeColumn { get; set; }
		public TestEnum EnumColumn { get; set; }
		public int? NullableIntColumn { get; set; }
		public decimal? NullableDecColumn { get; set; }
		public DateTime? NullableTimeColumn { get; set; }
		public TestEnum? NullableEnumColumn { get; set; }
		public string StrColumn { get; set; }
		public InternalTestType ClassColumn { get; set; }
		public InternalTestType2 StructColumn { get; set; }
	}

	class InternalTestType
	{
		public int InternalInt { get; set; }
	}

	struct InternalTestType2
	{
		public int InternalInt { get; set; }
	}
}
