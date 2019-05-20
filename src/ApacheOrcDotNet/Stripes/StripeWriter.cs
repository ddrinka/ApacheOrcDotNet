using ApacheOrcDotNet.ColumnTypes;
using ApacheOrcDotNet.FluentSerialization;
using ApacheOrcDotNet.Infrastructure;
using ApacheOrcDotNet.Statistics;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Stripes
{
    public class StripeWriter
    {
		readonly string _typeName;
		readonly Stream _outputStream;
		readonly bool _shouldAlignNumericValues;
		readonly double _uniqueStringThresholdRatio;
		readonly int _defaultDecimalPrecision;
		readonly int _defaultDecimalScale;
		readonly Compression.OrcCompressedBufferFactory _bufferFactory;
		readonly int _strideLength;
		readonly long _stripeLength;
		readonly SerializationConfiguration _serializationConfiguration;
		readonly List<ColumnWriterDetails> _columnWriters = new List<ColumnWriterDetails>();
		readonly List<Protocol.StripeStatistics> _stripeStats = new List<Protocol.StripeStatistics>();

		bool _rowAddingCompleted = false;
		int _rowsInStride = 0;
		long _rowsInStripe = 0;
		long _rowsInFile = 0;
		long _contentLength = 0;
		List<Protocol.StripeInformation> _stripeInformations = new List<Protocol.StripeInformation>();

		public StripeWriter(Type pocoType, Stream outputStream, bool shouldAlignNumericValues, double uniqueStringThresholdRatio, int defaultDecimalPrecision, int defaultDecimalScale, Compression.OrcCompressedBufferFactory bufferFactory, int strideLength, long stripeLength, SerializationConfiguration serializationConfiguration)
		{
			_typeName = pocoType.Name;
			_outputStream = outputStream;
			_shouldAlignNumericValues = shouldAlignNumericValues;
			_uniqueStringThresholdRatio = uniqueStringThresholdRatio;
			_defaultDecimalPrecision = defaultDecimalPrecision;
			_defaultDecimalScale = defaultDecimalScale;
			_bufferFactory = bufferFactory;
			_strideLength = strideLength;
			_stripeLength = stripeLength;
			_serializationConfiguration = serializationConfiguration;

			CreateColumnWriters(pocoType);
		}

		public void AddRows(IEnumerable<object> rows)
		{
			if (_rowAddingCompleted)
				throw new InvalidOperationException("Row adding as been completed");

			foreach(var row in rows)
			{
				foreach(var columnWriter in _columnWriters)
				{
					columnWriter.AddValueToState(row);
				}

				if(++_rowsInStride >= _strideLength)
					CompleteStride();
			}
		}

		public void RowAddingCompleted()
		{
			if (_rowsInStride != 0)
				CompleteStride();
			if (_rowsInStripe != 0)
				CompleteStripe();

			_contentLength = _outputStream.Position;
			_rowAddingCompleted = true;
		}

		public Protocol.Footer GetFooter()
		{
			if (!_rowAddingCompleted)
				throw new InvalidOperationException("Row adding not completed");

			return new Protocol.Footer
			{
				ContentLength = (ulong)_contentLength,
				NumberOfRows = (ulong)_rowsInFile,
				RowIndexStride = (uint)_strideLength,
				Stripes = _stripeInformations,
				Statistics = _columnWriters.Select(c => c.FileStatistics).ToList(),
				Types=GetColumnTypes().ToList()
			};
		}

		public Protocol.Metadata GetMetadata()
		{
			if (!_rowAddingCompleted)
				throw new InvalidOperationException("Row adding not completed");

			return new Protocol.Metadata
			{
				StripeStats = _stripeStats
			};
		}

		void CompleteStride()
		{
			foreach(var columnWriter in _columnWriters)
			{
				columnWriter.WriteValuesFromState();
			}

            _rowsInStripe += _rowsInStride;

            var totalStripeLength = _columnWriters.Sum(writer => writer.ColumnWriter.CompressedLength);
			if (totalStripeLength > _stripeLength)
				CompleteStripe();

            _rowsInStride = 0;
		}

		void CompleteStripe()
		{
			var stripeFooter = new Protocol.StripeFooter();
			var stripeStats = new Protocol.StripeStatistics();

			//Columns
			foreach(var writer in _columnWriters)
			{
				writer.ColumnWriter.FlushBuffers();
				var dictionaryLength = (writer.ColumnWriter as ColumnTypes.StringWriter)?.DictionaryLength ?? 0;	//DictionaryLength is only used by StringWriter
				stripeFooter.AddColumn(writer.ColumnWriter.ColumnEncoding, dictionaryLength);
			}

			var stripeInformation = new Protocol.StripeInformation();
			stripeInformation.Offset = (ulong)_outputStream.Position;
			stripeInformation.NumberOfRows = (ulong)_rowsInStripe;

			//Indexes
			foreach (var writer in _columnWriters)
			{
				//Write the index buffer
				var indexBuffer = _bufferFactory.CreateBuffer(Protocol.StreamKind.RowIndex);
                writer.ColumnWriter.Statistics.WriteToBuffer(indexBuffer, i => writer.ColumnWriter.Buffers[i].MustBeIncluded);
				indexBuffer.CopyTo(_outputStream);

				//Add the index to the footer
				stripeFooter.AddDataStream(writer.ColumnWriter.ColumnId, indexBuffer);

				//Collect summary statistics
				var columnStats = new ColumnStatistics();
				foreach (var stats in writer.ColumnWriter.Statistics)
				{
					stats.FillColumnStatistics(columnStats);
					stats.FillColumnStatistics(writer.FileStatistics);
				}
				stripeStats.ColStats.Add(columnStats);
			}
			_stripeStats.Add(stripeStats);

			stripeInformation.IndexLength = (ulong)_outputStream.Position - stripeInformation.Offset;

			//Data streams
			foreach (var writer in _columnWriters)
			{
				foreach (var buffer in writer.ColumnWriter.Buffers)
				{
					if (!buffer.MustBeIncluded)
						continue;
					buffer.CopyTo(_outputStream);
					stripeFooter.AddDataStream(writer.ColumnWriter.ColumnId, buffer);
				}
			}

			stripeInformation.DataLength = (ulong)_outputStream.Position - stripeInformation.IndexLength - stripeInformation.Offset;

			//Footer
			long footerLength;
			_bufferFactory.SerializeAndCompressTo(_outputStream, stripeFooter, out footerLength);
			stripeInformation.FooterLength = (ulong)footerLength;

			_stripeInformations.Add(stripeInformation);

			_rowsInFile += _rowsInStripe;
			_rowsInStripe = 0;
			foreach(var writer in _columnWriters)
			{
				writer.ColumnWriter.Reset();
			}
		}

		void CreateColumnWriters(Type type)
		{
			uint columnId = 1;
			foreach (var propertyInfo in GetPublicPropertiesFromPoco(type.GetTypeInfo()))
			{
				var propertyConfiguration = GetPropertyConfiguration(type, propertyInfo);
				if (propertyConfiguration != null && propertyConfiguration.ExcludeFromSerialization)
					continue;

				var columnWriterAndAction = GetColumnWriterDetails(propertyInfo, columnId++, propertyConfiguration);
				_columnWriters.Add(columnWriterAndAction);
			}
			_columnWriters.Insert(0, GetStructColumnWriter());		//Add the struct column at the beginning
		}

		IEnumerable<Protocol.ColumnType> GetColumnTypes()
		{
			foreach(var column in _columnWriters)
			{
				yield return column.ColumnType;
			}
		}

		static IEnumerable<PropertyInfo> GetPublicPropertiesFromPoco(TypeInfo pocoTypeInfo)
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

		SerializationPropertyConfiguration GetPropertyConfiguration(Type objectType, PropertyInfo propertyType)
		{
			if (_serializationConfiguration == null)
				return null;
			if (!_serializationConfiguration.Types.TryGetValue(objectType, out var typeConfiguration))
				return null;
			if (!typeConfiguration.Properties.TryGetValue(propertyType, out var propertyConfiguration))
				return null;
			return propertyConfiguration;
		}

		static bool IsNullable(TypeInfo propertyTypeInfo)
		{
			return propertyTypeInfo.IsGenericType 
				&& propertyTypeInfo.GetGenericTypeDefinition() == typeof(Nullable<>);
		}

		static T GetValue<T>(object classInstance, PropertyInfo property)
		{
			return (T)property.GetValue(classInstance);		//TODO make this emit IL to avoid the boxing of value-type T
		}

		ColumnWriterDetails GetColumnWriterDetails(PropertyInfo propertyInfo, uint columnId, SerializationPropertyConfiguration propertyConfiguration)
		{
			var propertyType = propertyInfo.PropertyType;

			//TODO move this to a pattern match switch
			if(propertyType == typeof(int))
				return GetColumnWriterDetails(GetLongColumnWriter(false, columnId), propertyInfo, classInstance => GetValue<int>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Int);
			if (propertyType == typeof(long))
				return GetColumnWriterDetails(GetLongColumnWriter(false, columnId), propertyInfo, classInstance => GetValue<long>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Long);
			if (propertyType == typeof(short))
				return GetColumnWriterDetails(GetLongColumnWriter(false, columnId), propertyInfo, classInstance => GetValue<short>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Short);
			if (propertyType == typeof(uint))
				return GetColumnWriterDetails(GetLongColumnWriter(false, columnId), propertyInfo, classInstance => GetValue<uint>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Int);
			if (propertyType == typeof(ulong))
				return GetColumnWriterDetails(GetLongColumnWriter(false, columnId), propertyInfo, classInstance => (long)GetValue<ulong>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Long);
			if (propertyType == typeof(ushort))
				return GetColumnWriterDetails(GetLongColumnWriter(false, columnId), propertyInfo, classInstance => GetValue<ushort>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Short);
			if (propertyType == typeof(int?))
				return GetColumnWriterDetails(GetLongColumnWriter(true, columnId), propertyInfo, classInstance => GetValue<int?>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Int);
			if (propertyType == typeof(long?))
				return GetColumnWriterDetails(GetLongColumnWriter(true, columnId), propertyInfo, classInstance => GetValue<long?>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Long);
			if (propertyType == typeof(short?))
				return GetColumnWriterDetails(GetLongColumnWriter(true, columnId), propertyInfo, classInstance => GetValue<short?>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Short);
			if (propertyType == typeof(uint?))
				return GetColumnWriterDetails(GetLongColumnWriter(true, columnId), propertyInfo, classInstance => GetValue<uint?>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Int);
			if (propertyType == typeof(ulong?))
				return GetColumnWriterDetails(GetLongColumnWriter(true, columnId), propertyInfo, classInstance => (long?)GetValue<ulong?>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Long);
			if (propertyType == typeof(ushort?))
				return GetColumnWriterDetails(GetLongColumnWriter(true, columnId), propertyInfo, classInstance => GetValue<ushort?>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Short);
			if (propertyType == typeof(byte))
				return GetColumnWriterDetails(GetByteColumnWriter(false, columnId), propertyInfo, classInstance => GetValue<byte>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Byte);
			if (propertyType == typeof(sbyte))
				return GetColumnWriterDetails(GetByteColumnWriter(false, columnId), propertyInfo, classInstance => (byte)GetValue<sbyte>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Byte);
			if (propertyType == typeof(byte?))
				return GetColumnWriterDetails(GetByteColumnWriter(true, columnId), propertyInfo, classInstance => GetValue<byte?>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Byte);
			if (propertyType == typeof(sbyte?))
				return GetColumnWriterDetails(GetByteColumnWriter(true, columnId), propertyInfo, classInstance => (byte?)GetValue<sbyte?>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Byte);
			if (propertyType == typeof(bool))
				return GetColumnWriterDetails(GetBooleanColumnWriter(false, columnId), propertyInfo, classInstance => GetValue<bool>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Boolean);
			if (propertyType == typeof(bool?))
				return GetColumnWriterDetails(GetBooleanColumnWriter(true, columnId), propertyInfo, classInstance => GetValue<bool?>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Boolean);
			if (propertyType == typeof(float))
				return GetColumnWriterDetails(GetFloatColumnWriter(false, columnId), propertyInfo, classInstance => GetValue<float>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Float);
			if (propertyType == typeof(float?))
				return GetColumnWriterDetails(GetFloatColumnWriter(true, columnId), propertyInfo, classInstance => GetValue<float?>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Float);
			if (propertyType == typeof(double))
				return GetColumnWriterDetails(GetDoubleColumnWriter(false, columnId), propertyInfo, classInstance => GetValue<double>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Double);
			if (propertyType == typeof(double?))
				return GetColumnWriterDetails(GetDoubleColumnWriter(true, columnId), propertyInfo, classInstance => GetValue<double?>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Double);
			if (propertyType == typeof(byte[]))
				return GetColumnWriterDetails(GetBinaryColumnWriter(columnId), propertyInfo, classInstance => GetValue<byte[]>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Binary);
			if (propertyType == typeof(decimal))
				return GetDecimalColumnWriterDetails(false, columnId, propertyInfo, classInstance => GetValue<decimal>(classInstance, propertyInfo), propertyConfiguration);
			if (propertyType == typeof(decimal?))
				return GetDecimalColumnWriterDetails(true, columnId, propertyInfo, classInstance => GetValue<decimal?>(classInstance, propertyInfo), propertyConfiguration);
			if (propertyType == typeof(DateTime) && propertyConfiguration!=null && propertyConfiguration.SerializeAsDate)
				return GetColumnWriterDetails(GetDateColumnWriter(false, columnId), propertyInfo, classInstance => GetValue<DateTime>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Date);
			if (propertyType == typeof(DateTime))
				return GetColumnWriterDetails(GetTimestampColumnWriter(false, columnId), propertyInfo, classInstance => GetValue<DateTime>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Timestamp);
			if (propertyType == typeof(DateTime?) && propertyConfiguration != null && propertyConfiguration.SerializeAsDate)
				return GetColumnWriterDetails(GetDateColumnWriter(true, columnId), propertyInfo, classInstance => GetValue<DateTime?>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Date);
			if (propertyType == typeof(DateTime?))
				return GetColumnWriterDetails(GetTimestampColumnWriter(true, columnId), propertyInfo, classInstance => GetValue<DateTime?>(classInstance, propertyInfo), Protocol.ColumnTypeKind.Timestamp);
			if (propertyType == typeof(DateTimeOffset))
				return GetColumnWriterDetails(GetTimestampColumnWriter(false, columnId), propertyInfo, classInstance => GetValue<DateTimeOffset>(classInstance, propertyInfo).UtcDateTime, Protocol.ColumnTypeKind.Timestamp);
			if (propertyType == typeof(DateTimeOffset?))
				return GetColumnWriterDetails(GetTimestampColumnWriter(false, columnId), propertyInfo, classInstance => GetValue<DateTimeOffset?>(classInstance, propertyInfo)?.UtcDateTime, Protocol.ColumnTypeKind.Timestamp);
			if (propertyType == typeof(string))
				return GetColumnWriterDetails(GetStringColumnWriter(columnId), propertyInfo, classInstance => GetValue<string>(classInstance, propertyInfo), Protocol.ColumnTypeKind.String);

			throw new NotImplementedException($"Only basic types are supported. Unable to handle type {propertyType}");
		}

		ColumnWriterDetails GetStructColumnWriter()
		{
			var columnWriter = new StructWriter(_bufferFactory, 0);
			var state = new List<object>();

			var structColumnType = new Protocol.ColumnType
			{
				Kind = Protocol.ColumnTypeKind.Struct
			};
			foreach (var column in _columnWriters)
			{
				structColumnType.FieldNames.Add(column.PropertyName);
				structColumnType.SubTypes.Add(column.ColumnWriter.ColumnId);
			}

			return new ColumnWriterDetails
			{
				PropertyName = _typeName,
				ColumnWriter = columnWriter,
				AddValueToState = classInstance =>
				{
					state.Add(classInstance);
				},
				WriteValuesFromState = () =>
				{
					columnWriter.AddBlock(state);
					state.Clear();
				},
				ColumnType = structColumnType
			};
		}

		ColumnWriterDetails GetColumnWriterDetails<T>(IColumnWriter<T> columnWriter, PropertyInfo propertyInfo, Func<object, T> valueGetter, Protocol.ColumnTypeKind columnKind)
		{
			var state = new List<T>();
			return new ColumnWriterDetails
			{
				PropertyName = propertyInfo.Name,
				ColumnWriter = columnWriter,
				AddValueToState = classInstance =>
				{
					var value = valueGetter(classInstance);
					state.Add(value);
				},
				WriteValuesFromState = () =>
				{
					columnWriter.AddBlock(state);
					state.Clear();
				},
				ColumnType = new Protocol.ColumnType
				{
					Kind = columnKind
				}
			};
		}

		ColumnWriterDetails GetDecimalColumnWriterDetails(bool isNullable, uint columnId, PropertyInfo propertyInfo, Func<object, decimal?> valueGetter, SerializationPropertyConfiguration propertyConfiguration)
		{
			var precision = propertyConfiguration?.DecimalPrecision ?? _defaultDecimalPrecision;
			var scale = propertyConfiguration?.DecimalScale ?? _defaultDecimalScale;

			var state = new List<decimal?>();
			var columnWriter = new DecimalWriter(isNullable, _shouldAlignNumericValues, precision, scale, _bufferFactory, columnId);
			return new ColumnWriterDetails
			{
				PropertyName = propertyInfo.Name,
				ColumnWriter = columnWriter,
				AddValueToState = classInstance =>
				{
					var value = valueGetter(classInstance);
					state.Add(value);
				},
				WriteValuesFromState = () =>
				{
					columnWriter.AddBlock(state);
					state.Clear();
				},
				ColumnType = new Protocol.ColumnType
				{
					Kind = Protocol.ColumnTypeKind.Decimal,
					Precision = (uint)precision,
					Scale = (uint)scale
				}
			};
		}

		IColumnWriter<long?> GetLongColumnWriter(bool isNullable, uint columnId)
		{
			return new LongWriter(isNullable, _shouldAlignNumericValues, _bufferFactory, columnId);
		}

		IColumnWriter<byte?> GetByteColumnWriter(bool isNullable, uint columnId)
		{
			return new ByteWriter(isNullable, _bufferFactory, columnId);
		}

		IColumnWriter<bool?> GetBooleanColumnWriter(bool isNullable, uint columnId)
		{
			return new BooleanWriter(isNullable, _bufferFactory, columnId);
		}

		IColumnWriter<float?> GetFloatColumnWriter(bool isNullable, uint columnId)
		{
			return new FloatWriter(isNullable, _bufferFactory, columnId);
		}

		IColumnWriter<double?> GetDoubleColumnWriter(bool isNullable, uint columnId)
		{
			return new DoubleWriter(isNullable, _bufferFactory, columnId);
		}

		IColumnWriter<byte[]> GetBinaryColumnWriter(uint columnId)
		{
			return new ColumnTypes.BinaryWriter(_shouldAlignNumericValues, _bufferFactory, columnId);
		}

		IColumnWriter<DateTime?> GetTimestampColumnWriter(bool isNullable, uint columnId)
		{
			return new TimestampWriter(isNullable, _shouldAlignNumericValues, _bufferFactory, columnId);
		}

		IColumnWriter<DateTime?> GetDateColumnWriter(bool isNullable, uint columnId)
		{
			return new DateWriter(isNullable, _shouldAlignNumericValues, _bufferFactory, columnId);
		}

		IColumnWriter<string> GetStringColumnWriter(uint columnId)
		{
			//TODO consider if we need separate configuration options for aligning lengths vs lookup values
			return new ColumnTypes.StringWriter(_shouldAlignNumericValues, _shouldAlignNumericValues, _uniqueStringThresholdRatio, _strideLength, _bufferFactory, columnId);
		}
	}

	class ColumnWriterDetails
	{
		public string PropertyName { get; set; }
		public IColumnWriter ColumnWriter { get; set; }
		public Action<object> AddValueToState { get; set; }
		public Action WriteValuesFromState { get; set; }
		public ColumnStatistics FileStatistics { get; } = new ColumnStatistics();
		public Protocol.ColumnType ColumnType { get; set; }
	}
}
