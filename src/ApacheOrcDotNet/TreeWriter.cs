using ApacheOrcDotNet.ColumnTypes;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace ApacheOrcDotNet
{
    public class TreeWriter
    {
		readonly bool _shouldAlignNumericValues;
		readonly Compression.OrcCompressedBufferFactory _bufferFactory;
		readonly int _strideLength;
		readonly List<ColumnWriterAndAction> _columnWriters = new List<ColumnWriterAndAction>();

		public TreeWriter(Type pocoType, Stream outputStream, bool shouldAlignNumericValues, Compression.OrcCompressedBufferFactory bufferFactory, int strideLength)
		{
			_shouldAlignNumericValues = shouldAlignNumericValues;
			_bufferFactory = bufferFactory;
			_strideLength = strideLength;

			CreateColumnWriters(pocoType);
		}

		void CreateColumnWriters(Type type)
		{
			foreach (var propertyInfo in GetPublicPropertiesFromPoco(type.GetTypeInfo()))
			{
				var columnWriterAndAction = GetColumnWriterAndAction(propertyInfo);
				_columnWriters.Add(columnWriterAndAction);
			}
		}

		IEnumerable<PropertyInfo> GetPublicPropertiesFromPoco(TypeInfo pocoTypeInfo)
		{
			if (pocoTypeInfo.BaseType != null)
				foreach (var property in GetPublicPropertiesFromPoco(pocoTypeInfo.BaseType.GetTypeInfo()))
					yield return property;

			foreach(var property in	pocoTypeInfo.DeclaredProperties)
			{
				if (property.GetMethod != null)
					yield return property;
			}
		}

		bool IsNullable(TypeInfo propertyTypeInfo)
		{
			return propertyTypeInfo.IsGenericType 
				&& propertyTypeInfo.GetGenericTypeDefinition() == typeof(Nullable<>);
		}

		T GetValue<T>(object classInstance, PropertyInfo property)
		{
			return (T)property.GetValue(classInstance);		//TODO make this emit IL to avoid the boxing of value-type T
		}

		ColumnWriterAndAction GetColumnWriterAndAction(PropertyInfo propertyInfo)
		{
			var propertyType = propertyInfo.PropertyType;

			if(propertyType==typeof(int))
				return GetLongColumnWriterAndAction(propertyInfo, classInstance => GetValue<int>(classInstance, propertyInfo));

			throw new NotImplementedException($"Only basic types are supported. Unable to handle type {propertyType}");
		}

		ColumnWriterAndAction GetLongColumnWriterAndAction(PropertyInfo propertyInfo, Func<object, long?> valueGetter)
		{
			var columnWriter = new LongWriter(false, _shouldAlignNumericValues, _bufferFactory);
			return new ColumnWriterAndAction
			{
				ColumnWriter = columnWriter,
				State = new List<long?>(),
				AddValuesToState = (state, classInstances) =>
				{
					var list = (List<long?>)state;
					foreach (var classInstance in classInstances)
					{
						var value = valueGetter(classInstance);
						list.Add(value);
					}
				},
				WriteValuesFromState = (state) =>
				{
					var list = (List<long?>)state;
					columnWriter.AddBlock(list);
				}
			};
		}
    }

	public delegate void AddValuesToState(object state, IEnumerable<object> classInstances);
	public delegate void WriteValuesFromState(object state);
	class ColumnWriterAndAction
	{
		public IColumnWriter ColumnWriter { get; set; }
		public AddValuesToState AddValuesToState { get; set; }
		public WriteValuesFromState WriteValuesFromState { get; set; }
		public object State { get; set; }

/*		public ColumnWriterAndAction(IColumnWriter writer, object state, AddValuesToState addValuesToState, WriteValuesFromState writeValuesFromState)
		{
			ColumnWriter = writer;
			AddValuesToState = addValuesToState;
			WriteValuesFromState = writeValuesFromState;
			State = state;
		}
*/
	}
}
