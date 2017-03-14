using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
    public class StringWriter : ColumnWriter<string>
    {
		readonly bool _shouldAlignLengths;
		readonly bool _shouldAlignDictionaryLookup;
		readonly double _uniqueStringThresholdRatio;
		readonly OrcCompressedBuffer _presentBuffer;
		readonly OrcCompressedBuffer _dataBuffer;
		readonly OrcCompressedBuffer _lengthBuffer;
		readonly OrcCompressedBuffer _dictionaryDataBuffer;

		public StringWriter(bool shouldAlignLengths, bool shouldAlignDictionaryLookup, double uniqueStringThresholdRatio, OrcCompressedBufferFactory bufferFactory, uint columnId)
			:base(bufferFactory, columnId)
		{
			_shouldAlignLengths = shouldAlignLengths;
			_shouldAlignDictionaryLookup = shouldAlignDictionaryLookup;
			_uniqueStringThresholdRatio = uniqueStringThresholdRatio;

			_presentBuffer = bufferFactory.CreateBuffer(StreamKind.Present);
			_presentBuffer.MustBeIncluded = false;
			_dataBuffer=bufferFactory.CreateBuffer(StreamKind.Data);
			_lengthBuffer = bufferFactory.CreateBuffer(StreamKind.Length);
			_dictionaryDataBuffer = bufferFactory.CreateBuffer(StreamKind.DictionaryData);
		}

		protected override ColumnEncodingKind DetectEncodingKind(IList<string> values)
		{
			var nonNullValues = values.Where(v => v != null);
			var uniqueValues = nonNullValues.Distinct().Count();
			var totalValues = nonNullValues.Count();
			if ((double)uniqueValues / (double)totalValues <= _uniqueStringThresholdRatio)
				return ColumnEncodingKind.DictionaryV2;
			else
				return ColumnEncodingKind.DirectV2;
		}

		protected override void AddDataStreamBuffers(IList<OrcCompressedBuffer> buffers, ColumnEncodingKind encodingKind)
		{
			switch (encodingKind)
			{
				case ColumnEncodingKind.DirectV2:
					buffers.Add(_presentBuffer);
					buffers.Add(_dataBuffer);
					buffers.Add(_lengthBuffer);
					break;
				case ColumnEncodingKind.DictionaryV2:
					buffers.Add(_presentBuffer);
					buffers.Add(_dataBuffer);
					buffers.Add(_lengthBuffer);
					buffers.Add(_dictionaryDataBuffer);
					break;
				default:
					throw new NotSupportedException($"Only DirectV2 and DictionaryV2 encodings are supported for {nameof(StringWriter)}");
			}
		}

		protected override IStatistics CreateStatistics() => new StringWriterStatistics();

		protected override void EncodeValues(IList<string> values, ColumnEncodingKind encodingKind, IStatistics statistics)
		{
			var stats = (StringWriterStatistics)statistics;

			if (encodingKind == ColumnEncodingKind.DirectV2)
			{
				var bytesList = new List<byte[]>(values.Count);
				var presentList = new List<bool>(values.Count);
				var lengthList = new List<long>(values.Count);

				foreach (var str in values)
				{
					stats.AddValue(str);
					if (str != null)
					{
						var bytes = Encoding.UTF8.GetBytes(str);
						bytesList.Add(bytes);
						lengthList.Add(bytes.Length);
					}
					presentList.Add(str != null);
				}

				var presentEncoder = new BitWriter(_presentBuffer);
				presentEncoder.Write(presentList);
				if (stats.HasNull)
					_presentBuffer.MustBeIncluded = true;

				foreach (var bytes in bytesList)
					_dataBuffer.Write(bytes, 0, bytes.Length);

				var lengthEncoder = new IntegerRunLengthEncodingV2Writer(_lengthBuffer);
				lengthEncoder.Write(lengthList, false, _shouldAlignLengths);
			}
			else if (encodingKind == ColumnEncodingKind.DictionaryV2)
			{
				var dictionaryBytesList = new List<byte[]>();
				var dictionaryLengthList = new List<long>();
				var presentList = new List<bool>(values.Count);
				var dictionaryLookupDataList = new List<long>(values.Count);

				//Generate the dictionary
				var unsortedDictionary = new Dictionary<string, DictionaryEntry>();     //We need a reference type to be able to set its value
				foreach (var str in values)
				{
					if (str != null)
						unsortedDictionary[str] = new DictionaryEntry();
				}

				//Sort the dictionary
				var sortedDictionary = new List<string>();
				int i = 0;
				foreach (var dictEntry in unsortedDictionary.OrderBy(d => d.Key, StringComparer.Ordinal))
				{
					sortedDictionary.Add(dictEntry.Key);
					dictEntry.Value.Id = i;
				}

				//Generate the lookup list
				foreach (var str in values)
				{
					stats.AddValue(str);
					if (str != null)
					{
						var id = unsortedDictionary[str].Id;
						dictionaryLookupDataList.Add(id);
					}
					presentList.Add(str != null);
				}

				//Prepare the dictionary values
				foreach (var dictEntry in sortedDictionary)
				{
					var bytes = Encoding.UTF8.GetBytes(dictEntry);
					dictionaryBytesList.Add(bytes);
					dictionaryLengthList.Add(bytes.Length);
				}

				//Write to the buffers
				var presentEncoder = new BitWriter(_presentBuffer);
				presentEncoder.Write(presentList);
				if (stats.HasNull)
					_presentBuffer.MustBeIncluded = true;

				var lookupEncoder = new IntegerRunLengthEncodingV2Writer(_dataBuffer);
				lookupEncoder.Write(dictionaryLookupDataList, false, _shouldAlignDictionaryLookup);

				foreach (var bytes in dictionaryBytesList)
					_dictionaryDataBuffer.Write(bytes, 0, bytes.Length);

				var dictionaryLengthEncoder = new IntegerRunLengthEncodingV2Writer(_lengthBuffer);
				dictionaryLengthEncoder.Write(dictionaryLengthList, false, _shouldAlignLengths);
			}
			else
				throw new ArgumentException();
		}

		class DictionaryEntry
		{
			public int Id { get; set; }
		}
	}
}
