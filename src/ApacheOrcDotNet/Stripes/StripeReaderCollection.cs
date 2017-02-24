using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Stripes
{
	public class StripeReaderCollection : IList<StripeReader>
	{
		readonly List<StripeReader> _innerCollection = new List<StripeReader>();
		public StripeReaderCollection(Stream inputStream, Protocol.Footer footer, Protocol.CompressionKind compressionKind)
		{
			foreach(var stripe in footer.Stripes)
			{
				_innerCollection.Add(new StripeReader(
					inputStream,
					stripe.Offset,
					stripe.IndexLength,
					stripe.Offset+stripe.IndexLength,
					stripe.DataLength,
					stripe.Offset+stripe.IndexLength+stripe.DataLength,
					stripe.FooterLength,
					stripe.NumberOfRows,
					compressionKind
					));
			}
		}

		#region IList Implementation
		public StripeReader this[int index]
		{
			get
			{
				return _innerCollection[index];
			}

			set
			{
				_innerCollection[index] = value;
			}
		}

		public int Count
		{
			get
			{
				return _innerCollection.Count;
			}
		}

		public bool IsReadOnly
		{
			get
			{
				return ((IList<StripeReader>)_innerCollection).IsReadOnly;
			}
		}

		public void Add(StripeReader item)
		{
			_innerCollection.Add(item);
		}

		public void Clear()
		{
			_innerCollection.Clear();
		}

		public bool Contains(StripeReader item)
		{
			return _innerCollection.Contains(item);
		}

		public void CopyTo(StripeReader[] array, int arrayIndex)
		{
			_innerCollection.CopyTo(array, arrayIndex);
		}

		public IEnumerator<StripeReader> GetEnumerator()
		{
			return ((IList<StripeReader>)_innerCollection).GetEnumerator();
		}

		public int IndexOf(StripeReader item)
		{
			return _innerCollection.IndexOf(item);
		}

		public void Insert(int index, StripeReader item)
		{
			_innerCollection.Insert(index, item);
		}

		public bool Remove(StripeReader item)
		{
			return _innerCollection.Remove(item);
		}

		public void RemoveAt(int index)
		{
			_innerCollection.RemoveAt(index);
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return ((IList<StripeReader>)_innerCollection).GetEnumerator();
		}
		#endregion
	}
}
