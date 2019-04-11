using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

namespace ApacheOrcDotNet
{
    public class OrcReader
    {
        readonly Type _type;
        readonly FileTail _fileTail;

        public OrcReader(Type type, Stream inputStream)
        {
            _type = type;
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
                var readers = properties.Select(p => new PropertyReader(stripeStreams, p.columnId, p.propertyInfo)).ToList();

                for (ulong i = 0; i < stripe.NumRows; i++)
                {
                    var obj = Activator.CreateInstance(_type);
                    foreach (var reader in readers)
                    {
                        reader.ReadNextValueAndSet(obj);
                    }
                    yield return obj;
                }
            }
        }

        static IEnumerable<(PropertyInfo propertyInfo, uint columnId)> FindColumnsForType(Type type, Protocol.Footer footer)
        {
            foreach (var property in GetWritablePublicProperties(type))
            {
                var columnId = footer.Types[0].FieldNames.FindIndex(fn => fn == property.Name) + 1;
                if (columnId == 0)
                    throw new KeyNotFoundException($"'{property.Name}' not found in ORC data");
                yield return (property, (uint)columnId);
            }
        }

        static IEnumerable<PropertyInfo> GetWritablePublicProperties(Type type)
        {
            return type.GetTypeInfo().DeclaredProperties.Where(p => p.SetMethod != null);
        }
    }

    class PropertyReader
    {
        readonly IEnumerator _columnEnumerator;
        readonly PropertyInfo _propertyInfo;

        public PropertyReader(Stripes.StripeStreamReaderCollection stripeStreams, uint columnId, PropertyInfo propertyInfo)
        {
            _columnEnumerator = GetColumnEnumerator(propertyInfo.PropertyType, stripeStreams, columnId);
            _propertyInfo = propertyInfo;
        }

        public void ReadNextValueAndSet(object obj)
        {
            if (!_columnEnumerator.MoveNext())
                throw new InvalidOperationException("Read past the end of data");
            _propertyInfo.SetValue(obj, _columnEnumerator.Current);
        }

        public IEnumerator GetColumnEnumerator(Type type, Stripes.StripeStreamReaderCollection stripeStreams, uint columnId)
        {
            if (type == typeof(int))
                return new ColumnTypes.LongReader(stripeStreams, columnId).Read().Select(val => (int)val).GetEnumerator();
            if (type == typeof(long))
                return new ColumnTypes.LongReader(stripeStreams, columnId).Read().Select(val => (long)val).GetEnumerator();
            if (type == typeof(short))
                return new ColumnTypes.LongReader(stripeStreams, columnId).Read().Select(val => (short)val).GetEnumerator();
            if (type == typeof(uint))
                return new ColumnTypes.LongReader(stripeStreams, columnId).Read().Select(val => (uint)val).GetEnumerator();
            if (type == typeof(ulong))
                return new ColumnTypes.LongReader(stripeStreams, columnId).Read().Select(val => (ulong)val).GetEnumerator();
            if (type == typeof(ushort))
                return new ColumnTypes.LongReader(stripeStreams, columnId).Read().Select(val => (ushort)val).GetEnumerator();
            if (type == typeof(int?))
                return new ColumnTypes.LongReader(stripeStreams, columnId).Read().Select(val => (int?)val).GetEnumerator();
            if (type == typeof(long?))
                return new ColumnTypes.LongReader(stripeStreams, columnId).Read().GetEnumerator();
            if (type == typeof(short?))
                return new ColumnTypes.LongReader(stripeStreams, columnId).Read().Select(val => (short?)val).GetEnumerator();
            if (type == typeof(uint?))
                return new ColumnTypes.LongReader(stripeStreams, columnId).Read().Select(val => (uint?)val).GetEnumerator();
            if (type == typeof(ulong?))
                return new ColumnTypes.LongReader(stripeStreams, columnId).Read().Select(val => (ulong?)val).GetEnumerator();
            if (type == typeof(ushort?))
                return new ColumnTypes.LongReader(stripeStreams, columnId).Read().Select(val => (ushort?)val).GetEnumerator();
            if (type == typeof(byte))
                return new ColumnTypes.ByteReader(stripeStreams, columnId).Read().Select(val => (byte)val).GetEnumerator();
            if (type == typeof(sbyte))
                return new ColumnTypes.ByteReader(stripeStreams, columnId).Read().Select(val => (sbyte)val).GetEnumerator();
            if (type == typeof(byte?))
                return new ColumnTypes.ByteReader(stripeStreams, columnId).Read().GetEnumerator();
            if (type == typeof(sbyte?))
                return new ColumnTypes.ByteReader(stripeStreams, columnId).Read().Select(val => (sbyte?)val).GetEnumerator();
            if (type == typeof(bool))
                return new ColumnTypes.BooleanReader(stripeStreams, columnId).Read().Select(val => (bool)val).GetEnumerator();
            if (type == typeof(bool?))
                return new ColumnTypes.BooleanReader(stripeStreams, columnId).Read().GetEnumerator();
            if (type == typeof(float))
                return new ColumnTypes.FloatReader(stripeStreams, columnId).Read().Select(val => (float)val).GetEnumerator();
            if (type == typeof(float?))
                return new ColumnTypes.FloatReader(stripeStreams, columnId).Read().GetEnumerator();
            if (type == typeof(double))
                return new ColumnTypes.DoubleReader(stripeStreams, columnId).Read().Select(val => (double)val).GetEnumerator();
            if (type == typeof(double?))
                return new ColumnTypes.DoubleReader(stripeStreams, columnId).Read().GetEnumerator();
            if (type == typeof(byte[]))
                return new ColumnTypes.BinaryReader(stripeStreams, columnId).Read().GetEnumerator();
            if (type == typeof(decimal))
                return new ColumnTypes.DecimalReader(stripeStreams, columnId).Read().Select(val => (decimal)val).GetEnumerator();
            if (type == typeof(decimal?))
                return new ColumnTypes.DecimalReader(stripeStreams, columnId).Read().GetEnumerator();

            //Handle DateTime--based on the ORC column type, read TimeStamp or Date

            if (type == typeof(DateTimeOffset))
                return new ColumnTypes.TimestampReader(stripeStreams, columnId).Read().Select(val => (DateTimeOffset)val).GetEnumerator();
            if (type == typeof(DateTimeOffset?))
                return new ColumnTypes.TimestampReader(stripeStreams, columnId).Read().Select(val => (DateTimeOffset?)val).GetEnumerator();
            if (type == typeof(string))
                return new ColumnTypes.StringReader(stripeStreams, columnId).Read().Select(val => val).GetEnumerator();

            throw new NotSupportedException($"Only basic types are supported. Unable to handle type {type}");
        }
    }
}
