using System.Collections.Generic;
using System.Reflection;

namespace ApacheOrcDotNet.FluentSerialization
{
    public interface ISerializationTypeConfiguration
    {
		IReadOnlyDictionary<PropertyInfo, SerializationPropertyConfiguration> Properties { get; }
    }
}
