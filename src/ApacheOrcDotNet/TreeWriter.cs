using ApacheOrcDotNet.ColumnTypes;
using ApacheOrcDotNet.Infrastructure;
using ApacheOrcDotNet.Statistics;
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
		readonly Stream _outputStream;
		readonly bool _shouldAlignNumericValues;
		readonly Compression.OrcCompressedBufferFactory _bufferFactory;
		readonly int _strideLength;
		readonly long _stripeLength;
		readonly List<ColumnWriterAndAction> _columnWriters = new List<ColumnWriterAndAction>();

		int _rowsInStride = 0;
		long _rowsInStripe = 0;
		List<Protocol.StripeInformation> _stripeInformations = new List<Protocol.StripeInformation>();

		public TreeWriter(Type pocoType, Stream outputStream, bool shouldAlignNumericValues, Compression.OrcCompressedBufferFactory bufferFactory, int strideLength, long stripeLength)
		{
			_outputStream = outputStream;
			_shouldAlignNumericValues = shouldAlignNumericValues;
			_bufferFactory = bufferFactory;
			_strideLength = strideLength;
			_stripeLength = stripeLength;

			CreateColumnWriters(pocoType);
		}

		public void AddRows(IEnumerable<object> rows)
		{
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
			foreach (var writer in _columnWriters)
			{
				writer.ColumnWriter.CompleteAddingBlocks();
				writer.ColumnWriter.FillStripeFooter(stripeFooter);
			}

			var stripeInformation = new Protocol.StripeInformation();
			stripeInformation.Offset = (ulong)_outputStream.Position;
			stripeInformation.NumberOfRows = (ulong)_rowsInStripe;

			//Indexes
			foreach (var writer in _columnWriters)
			{
				writer.ColumnWriter.CopyStatisticsTo(_outputStream);
				foreach (var stats in writer.ColumnWriter.Statistics)
				{
					stats.FillColumnStatistics(writer.StripeStatistics);
					stats.FillColumnStatistics(writer.FileStatistics);
				}
			}
			stripeInformation.IndexLength = (ulong)_outputStream.Position - stripeInformation.Offset;

			//Streams
			foreach(var writer in _columnWriters)
			{
				writer.ColumnWriter.CopyTo(_outputStream);
			}
			stripeInformation.DataLength = (ulong)_outputStream.Position - stripeInformation.IndexLength - stripeInformation.Offset;

			//Footer
			var footerBuffer = _bufferFactory.CreateBuffer(Protocol.StreamKind.Data);
			ProtoBuf.Serializer.Serialize(footerBuffer, stripeFooter);
			footerBuffer.WritingCompleted();
			footerBuffer.CompressedBuffer.CopyTo(_outputStream);
			stripeInformation.FooterLength = (ulong)_outputStream.Position - stripeInformation.DataLength - stripeInformation.IndexLength - stripeInformation.Offset;

			_stripeInformations.Add(stripeInformation);

			_rowsInStripe = 0;
			foreach(var writer in _columnWriters)
			{
				writer.ColumnWriter.Reset();
			}
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
			if (propertyType == typeof(long))
				return GetLongColumnWriterAndAction(propertyInfo, classInstance => GetValue<long>(classInstance, propertyInfo));
			if (propertyType == typeof(short))
				return GetLongColumnWriterAndAction(propertyInfo, classInstance => GetValue<short>(classInstance, propertyInfo));
			if (propertyType == typeof(uint))
				return GetLongColumnWriterAndAction(propertyInfo, classInstance => GetValue<uint>(classInstance, propertyInfo));
			if (propertyType == typeof(ulong))
				return GetLongColumnWriterAndAction(propertyInfo, classInstance => (long)GetValue<ulong>(classInstance, propertyInfo));
			if (propertyType == typeof(ushort))
				return GetLongColumnWriterAndAction(propertyInfo, classInstance => GetValue<ushort>(classInstance, propertyInfo));
			if (propertyType == typeof(int?))
				return GetLongColumnWriterAndAction(propertyInfo, classInstance => GetValue<int?>(classInstance, propertyInfo));
			if (propertyType == typeof(long?))
				return GetLongColumnWriterAndAction(propertyInfo, classInstance => GetValue<long?>(classInstance, propertyInfo));
			if (propertyType == typeof(short?))
				return GetLongColumnWriterAndAction(propertyInfo, classInstance => GetValue<short?>(classInstance, propertyInfo));
			if (propertyType == typeof(uint?))
				return GetLongColumnWriterAndAction(propertyInfo, classInstance => GetValue<uint?>(classInstance, propertyInfo));
			if (propertyType == typeof(ulong?))
				return GetLongColumnWriterAndAction(propertyInfo, classInstance => (long?)GetValue<ulong?>(classInstance, propertyInfo));
			if (propertyType == typeof(ushort?))
				return GetLongColumnWriterAndAction(propertyInfo, classInstance => GetValue<ushort?>(classInstance, propertyInfo));

			throw new NotImplementedException($"Only basic types are supported. Unable to handle type {propertyType}");
		}

		ColumnWriterAndAction GetLongColumnWriterAndAction(PropertyInfo propertyInfo, Func<object, long?> valueGetter)
		{
			var columnWriter = new LongWriter(false, _shouldAlignNumericValues, _bufferFactory);
			var state = new List<long?>();
			return new ColumnWriterAndAction
			{
				ColumnWriter = columnWriter,
				AddValueToState = classInstance =>
				{
					var value = valueGetter(classInstance);
					state.Add(value);
				},
				WriteValuesFromState = () =>
				{
					columnWriter.AddBlock(state);
				}
			};
		}
    }

	class ColumnWriterAndAction
	{
		public IColumnWriter ColumnWriter { get; set; }
		public Action<object> AddValueToState { get; set; }
		public Action WriteValuesFromState { get; set; }
		public ColumnStatistics StripeStatistics { get; } = new ColumnStatistics();
		public ColumnStatistics FileStatistics { get; } = new ColumnStatistics();
	}
}
