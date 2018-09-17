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
		readonly long _strideLength;
		readonly OrcCompressedBuffer _presentBuffer;
		readonly OrcCompressedBuffer _dataBuffer;
		readonly OrcCompressedBuffer _lengthBuffer;
		readonly OrcCompressedBuffer _dictionaryDataBuffer;
		readonly Dictionary<string, DictionaryEntry> _unsortedDictionary = new Dictionary<string, DictionaryEntry>();
		readonly List<DictionaryEntry> _dictionaryLookupValues = new List<DictionaryEntry>();


		public StringWriter(bool shouldAlignLengths, bool shouldAlignDictionaryLookup, double uniqueStringThresholdRatio, long strideLength, OrcCompressedBufferFactory bufferFactory, uint columnId)
		{
			_shouldAlignLengths = shouldAlignLengths;
			_shouldAlignDictionaryLookup = shouldAlignDictionaryLookup;
			_uniqueStringThresholdRatio = uniqueStringThresholdRatio;
			_strideLength = strideLength;
			ColumnId = columnId;

			_presentBuffer = bufferFactory.CreateBuffer(StreamKind.Present);
			_presentBuffer.MustBeIncluded = false;
			_dataBuffer = bufferFactory.CreateBuffer(StreamKind.Data);
			_lengthBuffer = bufferFactory.CreateBuffer(StreamKind.Length);
			_dictionaryDataBuffer = bufferFactory.CreateBuffer(StreamKind.DictionaryData);
		}

		public List<IStatistics> Statistics { get; } = new List<IStatistics>();
		public long CompressedLength
		{
			get
			{
				if (!ColumnEncodingIsValid())
					return 0;										//We haven't decided on an encoding yet
				else if (ColumnEncoding == ColumnEncodingKind.DirectV2)
					return Buffers.Sum(s => s.Length);				//We encode these as we go.  The buffer lengths are valid
				else if (ColumnEncoding == ColumnEncodingKind.DictionaryV2)
				{
					//Dictionary encoding doesn't flush to the buffers until a stripe is complete, but we don't know a stripe is complete without a sense of its compressed length
					if (_dictionaryDataBuffer.Length != 0)
						return Buffers.Sum(s => s.Length);			//The stripe is complete, we've flushed data, return the true size
					else
						return _dictionaryLookupValues.Count * 2;	//Make a wild approximation about how much data storage will be required for X values
				}
				else
					throw new InvalidOperationException();
			}
		}
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
		public uint DictionaryLength => (uint)_unsortedDictionary.Count;
		public ColumnEncodingKind ColumnEncoding { get; set; } = ColumnEncodingKind.Direct;    //Until we have a block of data to analyze, return the default
		bool ColumnEncodingIsValid() => ColumnEncoding == ColumnEncodingKind.DictionaryV2 || ColumnEncoding == ColumnEncodingKind.DirectV2;

		void EnsureEncodingKindIsSet(IList<string> values)
		{
			if (ColumnEncodingIsValid())
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
			if (ColumnEncoding == ColumnEncodingKind.DictionaryV2)
				WriteDictionaryEncodedData();
			foreach (var buffer in Buffers)
				buffer.Flush();
		}

		public void Reset()
		{
			_unsortedDictionary.Clear();
			_dictionaryLookupValues.Clear();
			foreach (var buffer in Buffers)
				buffer.Reset();
			_presentBuffer.MustBeIncluded = false;
			Statistics.Clear();
		}

		public void AddBlock(IList<string> values)
		{
			EnsureEncodingKindIsSet(values);

			if (ColumnEncoding == ColumnEncodingKind.DirectV2)
			{
				var stats = new StringWriterStatistics();
				Statistics.Add(stats);
				foreach (var buffer in Buffers)
					buffer.AnnotatePosition(stats, 0);      //Our implementation always ends the RLE at the stride

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
				foreach (var value in values)
				{
					if (value == null)
						_dictionaryLookupValues.Add(null);
					else
					{
						DictionaryEntry entry;
						if (!_unsortedDictionary.TryGetValue(value, out entry))
						{
							entry = new DictionaryEntry();
							_unsortedDictionary.Add(value, entry);
						}
						_dictionaryLookupValues.Add(entry);
					}
				}
			}
			else
				throw new ArgumentException();
		}

		void WriteDictionaryEncodedData()
		{
			//Sort the dictionary
			var sortedDictionary = new List<string>();
			int i = 0;
			foreach (var dictEntry in _unsortedDictionary.OrderBy(d => d.Key, StringComparer.Ordinal))
			{
				sortedDictionary.Add(dictEntry.Key);
				dictEntry.Value.Id = i++;
			}

			//Write the dictionary
			var dictionaryLengthList = new List<long>();
			foreach (var dictEntry in sortedDictionary)
			{
				var bytes = Encoding.UTF8.GetBytes(dictEntry);
				dictionaryLengthList.Add(bytes.Length);                 //Save the length
				_dictionaryDataBuffer.Write(bytes, 0, bytes.Length);    //Write to the buffer
			}

			//Write the dictionary lengths
			var dictionaryLengthEncoder = new IntegerRunLengthEncodingV2Writer(_lengthBuffer);
			dictionaryLengthEncoder.Write(dictionaryLengthList, false, _shouldAlignLengths);

			//Write the lookup values
			var presentList = new List<bool>(_dictionaryLookupValues.Count);
			var presentEncoder = new BitWriter(_presentBuffer);
			var lookupList = new List<long>(_dictionaryLookupValues.Count);
			var lookupEncoder = new IntegerRunLengthEncodingV2Writer(_dataBuffer);
			bool hasNull = false;
			int strideCount = 0;
			StringWriterStatistics stats = null;
			foreach (var value in _dictionaryLookupValues)
			{
				if(stats==null)
				{
					stats = new StringWriterStatistics();
					Statistics.Add(stats);
					foreach (var buffer in Buffers)
						buffer.AnnotatePosition(stats, 0);
				}

				if (value == null)
				{
					stats.AddValue(null);
					presentList.Add(false);
					hasNull = true;
				}
				else
				{
					var stringValue = sortedDictionary[value.Id];   //Look up the string value for this Id so we can notate statistics
					stats.AddValue(stringValue);
					presentList.Add(true);
					lookupList.Add(value.Id);
				}

				if (++strideCount == _strideLength)                  //If it's time for new statistics
				{
					//Flush to the buffers
					presentEncoder.Write(presentList);
					presentList.Clear();
					if (hasNull)
						_presentBuffer.MustBeIncluded = true;
					lookupEncoder.Write(lookupList, false, _shouldAlignDictionaryLookup);
					lookupList.Clear();

					strideCount = 0;
					stats = null;
				}
			}
		}

		class DictionaryEntry
		{
			public int Id { get; set; }
		}
	}
}