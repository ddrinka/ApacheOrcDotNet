using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Infrastructure
{
    public class ListSegment<T> : IList<T>
    {
		readonly IList<T> _underlyingList;
		readonly int _offset;
		readonly int _length;

		public ListSegment(IList<T> underlyingList, int offset) 
			: this(underlyingList, offset, int.MaxValue)
		{ }

		public ListSegment(IList<T> underlyingList, int offset, int length)
		{
			_underlyingList = underlyingList;
			_offset = offset;
			_length = length;
		}

		public int Count => Math.Min(_underlyingList.Count - _offset, _length);
		public bool IsReadOnly => true;
		T IList<T>.this[int index]
		{
			get
			{
				return _underlyingList[_offset + index];
			}
			set
			{
				throw new NotSupportedException();
			}
		}
		public IEnumerator<T> GetEnumerator()
		{
			return _underlyingList.Skip(_offset).Take(_length).GetEnumerator();		//TODO optimize this to avoid lengthy Skip operation
		}
		IEnumerator IEnumerable.GetEnumerator()
		{
			return GetEnumerator();
		}
		public int IndexOf(T item)
		{
			var result = _underlyingList.IndexOf(item);
			if (result >= _offset && result + _offset < Count)
				return result - _offset;
			else
				return -1;
		}
		public void Insert(int index, T item)
		{
			throw new NotSupportedException();
		}
		public void RemoveAt(int index)
		{
			throw new NotSupportedException();
		}
		public void Add(T item)
		{
			throw new NotSupportedException();
		}
		public void Clear()
		{
			throw new NotSupportedException();
		}
		public bool Contains(T item)
		{
			return IndexOf(item) != -1;
		}
		public void CopyTo(T[] array, int arrayIndex)
		{
			if (array.Length - arrayIndex < Count)
				throw new ArgumentException("Not enough room in destination array");

			int i = 0;
			foreach (var item in this)
			{
				array[i + arrayIndex] = item;
				i++;
			}
		}
		public bool Remove(T item)
		{
			throw new NotSupportedException();
		}
	}
}
