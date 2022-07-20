using System;
using System.Runtime.CompilerServices;

namespace ApacheOrcDotNet.OptimizedReader
{
    public ref struct BufferReader
    {
        private readonly ReadOnlySpan<byte> _buffer;
        private int _position;

        public BufferReader(ReadOnlySpan<byte> buffer)
        {
            _buffer = buffer;
            _position = 0;
        }

        public bool Complete => _position >= _buffer.Length;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryRead(out byte value)
        {
            if (_position >= _buffer.Length)
            {
                value = default;
                return false;
            }

            value = _buffer[_position];

            Advance(1);

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryReadTo(Span<byte> buffer)
        {
            if (_position + buffer.Length > _buffer.Length)
                return false;

            _buffer.Slice(_position, buffer.Length).CopyTo(buffer);

            Advance(buffer.Length);

            return true;
        }

        private void Advance(int length) 
            => _position += length;
    }
}
