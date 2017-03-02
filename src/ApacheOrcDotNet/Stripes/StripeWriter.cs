using ApacheOrcDotNet.ColumnTypes;
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
		readonly Compression.OrcCompressedBufferFactory _bufferFactory;
		readonly int _strideLength;
		readonly long _stripeLength;
		readonly List<ColumnWriterDetails> _columnWriters = new List<ColumnWriterDetails>();
		readonly List<Protocol.StripeStatistics> _stripeStats = new List<Protocol.StripeStatistics>();

		bool _rowAddingCompleted = false;
		int _rowsInStride = 0;
		long _rowsInStripe = 0;
		long _rowsInFile = 0;
		long _contentLength = 0;
		List<Protocol.StripeInformation> _stripeInformations = new List<Protocol.StripeInformation>();

		public StripeWriter(Type pocoType, Stream outputStream, bool shouldAlignNumericValues, Compression.OrcCompressedBufferFactory bufferFactory, int strideLength, long stripeLength)
		{
			_typeName = pocoType.Name;
			_outputStream = outputStream;
			_shouldAlignNumericValues = shouldAlignNumericValues;
			_bufferFactory = bufferFactory;
			_strideLength = strideLength;
			_stripeLength = stripeLength;

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

			var totalStripeLength = _columnWriters.Sum(writer => writer.ColumnWriter.CompressedLengths.Sum());
			if (totalStripeLength > _stripeLength)
				CompleteStripe();

			_rowsInStripe += _rowsInStride;
			_rowsInStride = 0;
		}

		void CompleteStripe()
		{
			var stripeFooter = new Protocol.StripeFooter();
			var stripeStats = new Protocol.StripeStatistics();

			foreach(var writer in _columnWriters)
			{
				writer.ColumnWriter.CompleteAddingBlocks();
				writer.ColumnWriter.FillColumnToStripeFooter(stripeFooter);
			}

			foreach(var writer in _columnWriters)
			{
				writer.ColumnWriter.FillIndexToStripeFooter(stripeFooter);

				var columnStats = new ColumnStatistics();
				foreach (var stats in writer.ColumnWriter.Statistics)
				{
					stats.FillColumnStatistics(columnStats);
					stats.FillColumnStatistics(writer.FileStatistics);
				}
				stripeStats.ColStats.Add(columnStats);
			}
			_stripeStats.Add(stripeStats);

			foreach (var writer in _columnWriters)
			{
				writer.ColumnWriter.FillDataToStripeFooter(stripeFooter);
			}

			var stripeInformation = new Protocol.StripeInformation();
			stripeInformation.Offset = (ulong)_outputStream.Position;
			stripeInformation.NumberOfRows = (ulong)_rowsInStripe;

			//Indexes
			foreach (var writer in _columnWriters)
			{
				writer.ColumnWriter.CopyIndexBufferTo(_outputStream);
			}
			stripeInformation.IndexLength = (ulong)_outputStream.Position - stripeInformation.Offset;

			//Streams
			foreach(var writer in _columnWriters)
			{
				writer.ColumnWriter.CopyDataBuffersTo(_outputStream);
			}
			stripeInformation.DataLength = (ulong)_outputStream.Position - stripeInformation.IndexLength - stripeInformation.Offset;

			//Footer
			var stripeFooterStream = _bufferFactory.SerializeAndCompress(stripeFooter);
			stripeFooterStream.CopyTo(_outputStream);
			stripeInformation.FooterLength = (ulong)_outputStream.Position - stripeInformation.DataLength - stripeInformation.IndexLength - stripeInformation.Offset;

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
				var columnWriterAndAction = GetColumnWriterDetails(propertyInfo, columnId++);
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

		static bool IsNullable(TypeInfo propertyTypeInfo)
		{
			return propertyTypeInfo.IsGenericType 
				&& propertyTypeInfo.GetGenericTypeDefinition() == typeof(Nullable<>);
		}

		static T GetValue<T>(object classInstance, PropertyInfo property)
		{
			return (T)property.GetValue(classInstance);		//TODO make this emit IL to avoid the boxing of value-type T
		}

		ColumnWriterDetails GetColumnWriterDetails(PropertyInfo propertyInfo, uint columnId)
		{
			var propertyType = propertyInfo.PropertyType;

			if(propertyType==typeof(int))
				return GetLongColumnWriterDetails(propertyInfo, classInstance => GetValue<int>(classInstance, propertyInfo), false, Protocol.ColumnTypeKind.Int, columnId);
			if (propertyType == typeof(long))
				return GetLongColumnWriterDetails(propertyInfo, classInstance => GetValue<long>(classInstance, propertyInfo), false, Protocol.ColumnTypeKind.Long, columnId);
			if (propertyType == typeof(short))
				return GetLongColumnWriterDetails(propertyInfo, classInstance => GetValue<short>(classInstance, propertyInfo), false, Protocol.ColumnTypeKind.Short, columnId);
			if (propertyType == typeof(uint))
				return GetLongColumnWriterDetails(propertyInfo, classInstance => GetValue<uint>(classInstance, propertyInfo), false, Protocol.ColumnTypeKind.Int, columnId);
			if (propertyType == typeof(ulong))
				return GetLongColumnWriterDetails(propertyInfo, classInstance => (long)GetValue<ulong>(classInstance, propertyInfo), false, Protocol.ColumnTypeKind.Long, columnId);
			if (propertyType == typeof(ushort))
				return GetLongColumnWriterDetails(propertyInfo, classInstance => GetValue<ushort>(classInstance, propertyInfo), false, Protocol.ColumnTypeKind.Short, columnId);
			if (propertyType == typeof(int?))
				return GetLongColumnWriterDetails(propertyInfo, classInstance => GetValue<int?>(classInstance, propertyInfo), true, Protocol.ColumnTypeKind.Int, columnId);
			if (propertyType == typeof(long?))
				return GetLongColumnWriterDetails(propertyInfo, classInstance => GetValue<long?>(classInstance, propertyInfo), true, Protocol.ColumnTypeKind.Long, columnId);
			if (propertyType == typeof(short?))
				return GetLongColumnWriterDetails(propertyInfo, classInstance => GetValue<short?>(classInstance, propertyInfo), false, Protocol.ColumnTypeKind.Short, columnId);
			if (propertyType == typeof(uint?))
				return GetLongColumnWriterDetails(propertyInfo, classInstance => GetValue<uint?>(classInstance, propertyInfo), true, Protocol.ColumnTypeKind.Int, columnId);
			if (propertyType == typeof(ulong?))
				return GetLongColumnWriterDetails(propertyInfo, classInstance => (long?)GetValue<ulong?>(classInstance, propertyInfo), true, Protocol.ColumnTypeKind.Long, columnId);
			if (propertyType == typeof(ushort?))
				return GetLongColumnWriterDetails(propertyInfo, classInstance => GetValue<ushort?>(classInstance, propertyInfo), true, Protocol.ColumnTypeKind.Short, columnId);

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

		ColumnWriterDetails GetLongColumnWriterDetails(PropertyInfo propertyInfo, Func<object, long?> valueGetter, bool isNullable, Protocol.ColumnTypeKind columnKind, uint columnId)
		{
			var columnWriter = new LongWriter(isNullable, _shouldAlignNumericValues, _bufferFactory, columnId);
			var state = new List<long?>();
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
