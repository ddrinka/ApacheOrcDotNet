using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace ApacheOrcDotNet
{
    public class OrcReader
    {
        readonly Type _type;
        readonly FileTail _fileTail;
        readonly bool _ignoreMissingColumns;

        public OrcReader(Type type, Stream inputStream, bool ignoreMissingColumns = false)
        {
            _type = type;
            _ignoreMissingColumns = ignoreMissingColumns;
            _fileTail = new FileTail(inputStream);

            if (_fileTail.Footer.Types[0].Kind != Protocol.ColumnTypeKind.Struct)
                throw new InvalidDataException($"The base type must be {nameof(Protocol.ColumnTypeKind.Struct)}");
        }

        public IEnumerable<object> Read()
        {
            var properties = FindColumnsForType(_type, _fileTail.Footer).ToList();

            foreach (var stripe in _fileTail.Stripes)
            {
                var stripeStreams = stripe.GetStripeStreamCollection();
                var readAndSetters = properties.Select(p => GetReadAndSetterForColumn(p.propertyInfo, stripeStreams, p.columnId, p.columnType)).ToList();

                for (ulong i = 0; i < stripe.NumRows; i++)
                {
                    var obj = Activator.CreateInstance(_type);
                    foreach (var readAndSetter in readAndSetters)
                    {
                        readAndSetter(obj);
                    }
                    yield return obj;
                }
            }
        }

        IEnumerable<(PropertyInfo propertyInfo, uint columnId, Protocol.ColumnTypeKind columnType)> FindColumnsForType(Type type, Protocol.Footer footer)
        {
            foreach (var property in GetWritablePublicProperties(type))
            {
                var columnId = footer.Types[0].FieldNames.FindIndex(fn => fn.ToLower() == property.Name.ToLower()) + 1;
                if (columnId == 0)
                {
                    if (_ignoreMissingColumns)
                        continue;
                    else
                        throw new KeyNotFoundException($"'{property.Name}' not found in ORC data");
                }
                var columnType = footer.Types[columnId].Kind;
                yield return (property, (uint)columnId, columnType);
            }
        }

        static IEnumerable<PropertyInfo> GetWritablePublicProperties(Type type)
        {
            return type.GetTypeInfo().DeclaredProperties.Where(p => p.SetMethod != null);
        }

        Action<object> GetReadAndSetterForColumn(PropertyInfo propertyInfo, Stripes.StripeStreamReaderCollection stripeStreams, uint columnId, Protocol.ColumnTypeKind columnType)
        {
            switch (columnType)
            {
                case Protocol.ColumnTypeKind.Long:
                case Protocol.ColumnTypeKind.Int:
                case Protocol.ColumnTypeKind.Short:
                    return GetValueSetterEnumerable(propertyInfo, new ColumnTypes.LongReader(stripeStreams, columnId).Read());
                case Protocol.ColumnTypeKind.Byte:
                    return GetValueSetterEnumerable(propertyInfo, new ColumnTypes.ByteReader(stripeStreams, columnId).Read());
                case Protocol.ColumnTypeKind.Boolean:
                    return GetValueSetterEnumerable(propertyInfo, new ColumnTypes.BooleanReader(stripeStreams, columnId).Read());
                case Protocol.ColumnTypeKind.Float:
                    return GetValueSetterEnumerable(propertyInfo, new ColumnTypes.FloatReader(stripeStreams, columnId).Read());
                case Protocol.ColumnTypeKind.Double:
                    return GetValueSetterEnumerable(propertyInfo, new ColumnTypes.DoubleReader(stripeStreams, columnId).Read());
                case Protocol.ColumnTypeKind.Binary:
                    return GetValueSetterEnumerable(propertyInfo, new ColumnTypes.BinaryReader(stripeStreams, columnId).Read());
                case Protocol.ColumnTypeKind.Decimal:
                    return GetValueSetterEnumerable(propertyInfo, new ColumnTypes.DecimalReader(stripeStreams, columnId).Read());
                case Protocol.ColumnTypeKind.Timestamp:
                    return GetValueSetterEnumerable(propertyInfo, new ColumnTypes.TimestampReader(stripeStreams, columnId).Read());
                case Protocol.ColumnTypeKind.Date:
                    return GetValueSetterEnumerable(propertyInfo, new ColumnTypes.DateReader(stripeStreams, columnId).Read());
                case Protocol.ColumnTypeKind.String:
                    return GetValueSetterEnumerable(propertyInfo, new ColumnTypes.StringReader(stripeStreams, columnId).Read());
                default:
                    throw new NotImplementedException($"Column type {columnType} is not supported");
            }
        }

        static Action<object> GetValueSetterEnumerable<T>(PropertyInfo propertyInfo, IEnumerable<T> enumerable)
        {
            var valueSetter = GetValueSetter<T>(propertyInfo);
            var enumerator = enumerable.GetEnumerator();
            return instance =>
            {
                if (!enumerator.MoveNext())
                    throw new InvalidOperationException("Read past the end of data");
                valueSetter(instance, enumerator.Current);
            };
        }

        static Action<object, FromT> GetValueSetter<FromT>(PropertyInfo propertyInfo)
        {
            var instance = Expression.Parameter(typeof(object), "instance");
            var value = Expression.Parameter(typeof(FromT), "value");
            var valueAsType = Expression.Convert(value, propertyInfo.PropertyType);
            var instanceAsType = Expression.Convert(instance, propertyInfo.DeclaringType);
            var callSetter = Expression.Call(instanceAsType, propertyInfo.GetSetMethod(), valueAsType);
            var parameters = new ParameterExpression[] { instance, value };

            return Expression.Lambda<Action<object, FromT>>(callSetter, parameters).Compile();
        }
    }
}
