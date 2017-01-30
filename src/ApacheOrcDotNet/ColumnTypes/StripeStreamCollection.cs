using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using ApacheOrcDotNet.Protocol;

namespace ApacheOrcDotNet.ColumnTypes
{
	using IOStream = System.IO.Stream;

	public class StripeStreamCollection : IList<StripeStream>
    {
		public List<StripeStream> _underlyingCollection = new List<StripeStream>();

		public StripeStreamCollection(IOStream inputStream, StripeFooter stripeFooter, long stripeOffset, CompressionKind compressionKind)
		{
			long offset = stripeOffset;
			foreach(var stream in stripeFooter.Streams)
			{
				if (_underlyingCollection.Exists(s => s.ColumnId == stream.Column && s.StreamKind == stream.Kind))
					throw new InvalidOperationException($"More than one stream matching {nameof(stream.Column)} ({stream.Column}) and {nameof(stream.Kind)} ({stream.Kind}) found in {nameof(StripeFooter)}");

				var column = stripeFooter.Columns[(int)stream.Column];

				_underlyingCollection.Add(new StripeStream(
					inputStream,
					stream.Column,
					stream.Kind,
					column.Kind,
					offset,
					stream.Length,
					compressionKind
					));
			}
		}

		#region IList Implementation
		public StripeStream this[int index]
		{
			get
			{
				return ((IList<StripeStream>)_underlyingCollection)[index];
			}

			set
			{
				((IList<StripeStream>)_underlyingCollection)[index] = value;
			}
		}

		public int Count
		{
			get
			{
				return ((IList<StripeStream>)_underlyingCollection).Count;
			}
		}

		public bool IsReadOnly
		{
			get
			{
				return ((IList<StripeStream>)_underlyingCollection).IsReadOnly;
			}
		}

		public void Add(StripeStream item)
		{
			((IList<StripeStream>)_underlyingCollection).Add(item);
		}

		public void Clear()
		{
			((IList<StripeStream>)_underlyingCollection).Clear();
		}

		public bool Contains(StripeStream item)
		{
			return ((IList<StripeStream>)_underlyingCollection).Contains(item);
		}

		public void CopyTo(StripeStream[] array, int arrayIndex)
		{
			((IList<StripeStream>)_underlyingCollection).CopyTo(array, arrayIndex);
		}

		public IEnumerator<StripeStream> GetEnumerator()
		{
			return ((IList<StripeStream>)_underlyingCollection).GetEnumerator();
		}

		public int IndexOf(StripeStream item)
		{
			return ((IList<StripeStream>)_underlyingCollection).IndexOf(item);
		}

		public void Insert(int index, StripeStream item)
		{
			((IList<StripeStream>)_underlyingCollection).Insert(index, item);
		}

		public bool Remove(StripeStream item)
		{
			return ((IList<StripeStream>)_underlyingCollection).Remove(item);
		}

		public void RemoveAt(int index)
		{
			((IList<StripeStream>)_underlyingCollection).RemoveAt(index);
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return ((IList<StripeStream>)_underlyingCollection).GetEnumerator();
		}
		#endregion
	}
}
