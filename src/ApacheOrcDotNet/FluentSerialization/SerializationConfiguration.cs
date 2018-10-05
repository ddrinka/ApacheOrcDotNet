using System;
using System.Collections.Generic;
using System.Text;

namespace ApacheOrcDotNet.FluentSerialization
{
    public class SerializationConfiguration
    {
		readonly Dictionary<Type, ISerializationTypeConfiguration> _types = new Dictionary<Type, ISerializationTypeConfiguration>();

		public IReadOnlyDictionary<Type, ISerializationTypeConfiguration> Types { get => _types; }

		public SerializationTypeConfiguration<T> ConfigureType<T>()
		{
			if(!_types.TryGetValue(typeof(T), out var typeConfiguration))
			{
				typeConfiguration = new SerializationTypeConfiguration<T>(this);
				_types.Add(typeof(T), typeConfiguration);
			}
			return (SerializationTypeConfiguration<T>)typeConfiguration;
		}
    }
}
