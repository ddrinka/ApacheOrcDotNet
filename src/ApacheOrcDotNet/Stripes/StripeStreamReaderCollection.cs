using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

namespace ApacheOrcDotNet.Stripes
{
	public class StripeStreamReaderCollection : IList<StripeStreamReader>
    {
		readonly List<StripeStreamReader> _underlyingCollection = new List<StripeStreamReader>();

		internal StripeStreamReaderCollection(Stream inputStream, Protocol.StripeFooter stripeFooter, long stripeOffset, Protocol.CompressionKind compressionKind)
		{
			long offset = stripeOffset;
			foreach(var stream in stripeFooter.Streams)
			{
				if (_underlyingCollection.Exists(s => s.ColumnId == stream.Column && s.StreamKind == stream.Kind))
					throw new InvalidOperationException($"More than one stream matching {nameof(stream.Column)} ({stream.Column}) and {nameof(stream.Kind)} ({stream.Kind}) found in {nameof(Protocol.StripeFooter)}");

				var column = stripeFooter.Columns[(int)stream.Column];

				_underlyingCollection.Add(new StripeStreamReader(
					inputStream,
					stream.Column,
					stream.Kind,
					column.Kind,
					offset,
					stream.Length,
					compressionKind
					));

				offset += (long)stream.Length;
			}
		}

		#region IList Implementation
		public StripeStreamReader this[int index]
		{
			get
			{
				return ((IList<StripeStreamReader>)_underlyingCollection)[index];
			}

			set
			{
				((IList<StripeStreamReader>)_underlyingCollection)[index] = value;
			}
		}

		public int Count
		{
			get
			{
				return ((IList<StripeStreamReader>)_underlyingCollection).Count;
			}
		}

		public bool IsReadOnly
		{
			get
			{
				return ((IList<StripeStreamReader>)_underlyingCollection).IsReadOnly;
			}
		}

		public void Add(StripeStreamReader item)
		{
			((IList<StripeStreamReader>)_underlyingCollection).Add(item);
		}

		public void Clear()
		{
			((IList<StripeStreamReader>)_underlyingCollection).Clear();
		}

		public bool Contains(StripeStreamReader item)
		{
			return ((IList<StripeStreamReader>)_underlyingCollection).Contains(item);
		}

		public void CopyTo(StripeStreamReader[] array, int arrayIndex)
		{
			((IList<StripeStreamReader>)_underlyingCollection).CopyTo(array, arrayIndex);
		}

		public IEnumerator<StripeStreamReader> GetEnumerator()
		{
			return ((IList<StripeStreamReader>)_underlyingCollection).GetEnumerator();
		}

		public int IndexOf(StripeStreamReader item)
		{
			return ((IList<StripeStreamReader>)_underlyingCollection).IndexOf(item);
		}

		public void Insert(int index, StripeStreamReader item)
		{
			((IList<StripeStreamReader>)_underlyingCollection).Insert(index, item);
		}

		public bool Remove(StripeStreamReader item)
		{
			return ((IList<StripeStreamReader>)_underlyingCollection).Remove(item);
		}

		public void RemoveAt(int index)
		{
			((IList<StripeStreamReader>)_underlyingCollection).RemoveAt(index);
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return ((IList<StripeStreamReader>)_underlyingCollection).GetEnumerator();
		}
		#endregion
	}
}
