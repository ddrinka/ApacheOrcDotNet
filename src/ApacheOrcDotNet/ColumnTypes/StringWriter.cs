using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace ApacheOrcDotNet.ColumnTypes
{
	public class StringWriter : IColumnWriter<string>
	{
		readonly bool _shouldAlignLengths;
		readonly bool _shouldAlignDictionaryLookup;
		readonly double _uniqueStringThresholdRatio;
		readonly OrcCompressedBuffer _presentBuffer;
		readonly OrcCompressedBuffer _dataBuffer;
		readonly OrcCompressedBuffer _lengthBuffer;
		readonly OrcCompressedBuffer _dictionaryDataBuffer;

		public StringWriter(bool shouldAlignLengths, bool shouldAlignDictionaryLookup, double uniqueStringThresholdRatio, OrcCompressedBufferFactory bufferFactory, uint columnId)
		{
			_shouldAlignLengths = shouldAlignLengths;
			_shouldAlignDictionaryLookup = shouldAlignDictionaryLookup;
			_uniqueStringThresholdRatio = uniqueStringThresholdRatio;
			ColumnId = columnId;

			_presentBuffer = bufferFactory.CreateBuffer(StreamKind.Present);
			_presentBuffer.MustBeIncluded = false;
			_dataBuffer = bufferFactory.CreateBuffer(StreamKind.Data);
			_lengthBuffer = bufferFactory.CreateBuffer(StreamKind.Length);
			_dictionaryDataBuffer = bufferFactory.CreateBuffer(StreamKind.DictionaryData);
		}

		public List<IStatistics> Statistics { get; } = new List<IStatistics>();
		public long CompressedLength => Buffers.Sum(s => s.Length);
		public uint ColumnId { get; }
		public IEnumerable<OrcCompressedBuffer> Buffers
		{
			get
			{
				switch (ColumnEncoding)
				{
					case ColumnEncodingKind.DirectV2:
						return new[] { _presentBuffer, _dataBuffer, _lengthBuffer };
					case ColumnEncodingKind.DictionaryV2:
						return new[] { _presentBuffer, _dataBuffer, _lengthBuffer, _dictionaryDataBuffer };
					default:
						throw new NotSupportedException($"Only DirectV2 and DictionaryV2 encodings are supported for {nameof(StringWriter)}");
				}
			}
		}
		public uint DictionaryLength => (uint)_dictionaryDataBuffer.Length;
		public ColumnEncodingKind ColumnEncoding { get; set; } = ColumnEncodingKind.Direct;    //Until we have a block of data to analyze, return the default

		void EnsureEncodingKindIsSet(IList<string> values)
		{
			if (ColumnEncoding == ColumnEncodingKind.DictionaryV2 || ColumnEncoding == ColumnEncodingKind.DirectV2)
				return;

			//Detect the encoding type
			var nonNullValues = values.Where(v => v != null);
			var uniqueValues = nonNullValues.Distinct().Count();
			var totalValues = nonNullValues.Count();
			if ((double)uniqueValues / (double)totalValues <= _uniqueStringThresholdRatio)
				ColumnEncoding = ColumnEncodingKind.DictionaryV2;
			else
				ColumnEncoding = ColumnEncodingKind.DirectV2;
		}

		public void FlushBuffers()
		{
			foreach (var buffer in Buffers)
				buffer.Flush();
		}

		public void Reset()
		{
			foreach (var buffer in Buffers)
				buffer.Reset();
			_presentBuffer.MustBeIncluded = false;
			Statistics.Clear();
		}

		public void AddBlock(IList<string> values)
		{
			EnsureEncodingKindIsSet(values);

			var stats = new StringWriterStatistics();
			Statistics.Add(stats);
			foreach (var buffer in Buffers)
				buffer.AnnotatePosition(stats, 0);      //Our implementation always ends the RLE at the stride

			if (ColumnEncoding == ColumnEncodingKind.DirectV2)
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
			else if (ColumnEncoding == ColumnEncodingKind.DictionaryV2)
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
					dictEntry.Value.Id = i++;
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