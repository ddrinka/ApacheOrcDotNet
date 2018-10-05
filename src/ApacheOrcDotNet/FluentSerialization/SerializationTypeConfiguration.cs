using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace ApacheOrcDotNet.FluentSerialization
{
    public class SerializationTypeConfiguration<T> : ISerializationTypeConfiguration
    {
		readonly SerializationConfiguration _root;
		readonly Dictionary<PropertyInfo, SerializationPropertyConfiguration> _properties = new Dictionary<PropertyInfo, SerializationPropertyConfiguration>();

		public SerializationTypeConfiguration(SerializationConfiguration root)
		{
			_root = root;
		}

		public IReadOnlyDictionary<PropertyInfo, SerializationPropertyConfiguration> Properties { get => _properties; }

		internal void AddConfiguration(PropertyInfo propertyInfo, SerializationPropertyConfiguration configuration) => _properties.Add(propertyInfo, configuration);
		internal SerializationConfiguration Root { get => _root; }
	}

	public static class SerializationTypeConfigurationExtensions
	{
		public static SerializationTypeConfiguration<T> ConfigureProperty<T, TProp>(this SerializationTypeConfiguration<T> typeConfiguration, Expression<Func<T, TProp>> expr, Action<SerializationPropertyConfiguration> configBuilder)
		{
			var propertyInfo = GetPropertyInfoFromExpression(expr);
			var config = new SerializationPropertyConfiguration();
			configBuilder(config);
			typeConfiguration.AddConfiguration(propertyInfo, config);
			return typeConfiguration;
		}

		public static SerializationConfiguration Build<T>(this SerializationTypeConfiguration<T> typeConfiguration)
		{
			return typeConfiguration.Root;
		}

		static PropertyInfo GetPropertyInfoFromExpression(LambdaExpression expr)
		{
			if (expr.Body.NodeType != ExpressionType.MemberAccess)
				throw new NotSupportedException("Fluent interface only supports simple expression identifiers");

			var member = (MemberExpression)expr.Body;
			return (PropertyInfo)member.Member;
		}
	}
}
