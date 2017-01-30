using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet
{
	public class StripeCollection : IList<Stripe>
	{
		readonly List<Stripe> _innerCollection = new List<Stripe>();
		internal StripeCollection(Stream inputStream, Protocol.Footer footer, Protocol.CompressionKind compressionKind)
		{
			foreach(var stripe in footer.Stripes)
			{
				_innerCollection.Add(new Stripe(
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
		public Stripe this[int index]
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
				return ((IList<Stripe>)_innerCollection).IsReadOnly;
			}
		}

		public void Add(Stripe item)
		{
			_innerCollection.Add(item);
		}

		public void Clear()
		{
			_innerCollection.Clear();
		}

		public bool Contains(Stripe item)
		{
			return _innerCollection.Contains(item);
		}

		public void CopyTo(Stripe[] array, int arrayIndex)
		{
			_innerCollection.CopyTo(array, arrayIndex);
		}

		public IEnumerator<Stripe> GetEnumerator()
		{
			return ((IList<Stripe>)_innerCollection).GetEnumerator();
		}

		public int IndexOf(Stripe item)
		{
			return _innerCollection.IndexOf(item);
		}

		public void Insert(int index, Stripe item)
		{
			_innerCollection.Insert(index, item);
		}

		public bool Remove(Stripe item)
		{
			return _innerCollection.Remove(item);
		}

		public void RemoveAt(int index)
		{
			_innerCollection.RemoveAt(index);
		}

		IEnumerator IEnumerable.GetEnumerator()
		{
			return ((IList<Stripe>)_innerCollection).GetEnumerator();
		}
		#endregion
	}
}
