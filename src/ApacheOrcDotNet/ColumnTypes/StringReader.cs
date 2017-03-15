using ApacheOrcDotNet.Stripes;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
	public class StringReader : ColumnReader
	{
		public StringReader(StripeStreamReaderCollection stripeStreams, uint columnId) : base(stripeStreams, columnId)
		{
		}

		public IEnumerable<string> Read()
		{
			var kind = GetColumnEncodingKind(Protocol.StreamKind.Data);
			switch (kind)
			{
				case Protocol.ColumnEncodingKind.DirectV2: return ReadDirectV2();
				case Protocol.ColumnEncodingKind.DictionaryV2: return ReadDictionaryV2();
				default: throw new NotImplementedException($"Unsupported column encoding {kind}");
			}
		}

		IEnumerable<string> ReadDirectV2()
		{
			var present = ReadBooleanStream(Protocol.StreamKind.Present);
			var data = ReadBinaryStream(Protocol.StreamKind.Data);
			var length = ReadNumericStream(Protocol.StreamKind.Length, false);
			if (data == null || length == null)
				throw new InvalidDataException("DATA and LENGTH streams must be available");

			int stringOffset = 0;
			if(present==null)
			{
				foreach(var len in length)
				{
					var value = Encoding.UTF8.GetString(data, stringOffset, (int)len);
					stringOffset += (int)len;
					yield return value;
				}
			}
			else
			{
				var lengthEnumerator = ((IEnumerable<long>)length).GetEnumerator();
				foreach(var isPresent in present)
				{
					if (isPresent)
					{
						var success = lengthEnumerator.MoveNext();
						if (!success)
							throw new InvalidDataException("The PRESENT data stream's length didn't match the LENGTH stream's length");
						var len = lengthEnumerator.Current;
						var value = Encoding.UTF8.GetString(data, stringOffset, (int)len);
						stringOffset += (int)len;
						yield return value;
					}
					else
						yield return null;
				}
			}
		}

		IEnumerable<string> ReadDictionaryV2()
		{
			var present = ReadBooleanStream(Protocol.StreamKind.Present);
			var data = ReadNumericStream(Protocol.StreamKind.Data, false);
			var dictionaryData = ReadBinaryStream(Protocol.StreamKind.DictionaryData);
			var length = ReadNumericStream(Protocol.StreamKind.Length, false);
			if (data == null || dictionaryData == null || length == null)
				throw new InvalidDataException("DATA, DICTIONARY_DATA, and LENGTH streams must be available");

			var dictionary = new List<string>();
			int stringOffset = 0;
			foreach(var len in length)
			{
				var dictionaryValue = Encoding.UTF8.GetString(dictionaryData, stringOffset, (int)len);
				stringOffset += (int)len;
				dictionary.Add(dictionaryValue);
			}

			if(present==null)
			{
				foreach (var value in data)
					yield return dictionary[(int)value];
			}
			else
			{
				var valueEnumerator = ((IEnumerable<long>)data).GetEnumerator();
				foreach(var isPresent in present)
				{
					if (isPresent)
					{
						var success = valueEnumerator.MoveNext();
						if (!success)
							throw new InvalidDataException("The PRESENT data stream's length didn't match the DATA stream's length");
						yield return dictionary[(int)valueEnumerator.Current];
					}
					else
						yield return null;
				}
			}
		}
	}
}
