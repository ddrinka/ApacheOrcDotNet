using ApacheOrcDotNet.Compression;
using ApacheOrcDotNet.Protocol;
using System;
using System.Buffers;

namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public sealed class DecompressingMemorySequence : IDisposable
    {
        readonly SequenceNode _begin;
        readonly SequenceNode _end;

        public DecompressingMemorySequence(ReadOnlySpan<byte> compressedBuffer, CompressionKind compressionKind, int compressionBlockSize)
        {
            int position = 0;
            while (position < compressedBuffer.Length)
            {
                var compressedChunkLength = OrcCompressedBlock.GetChunkLength(compressionKind, compressedBuffer[position..]);
                var decompressedSegment = ArrayPool<byte>.Shared.Rent(compressionBlockSize);
                var decompressedLength = OrcCompressedBlock.DecompressBlock(compressionKind, compressedBuffer.Slice(position, compressedChunkLength), decompressedSegment);
                
                _end = new SequenceNode(decompressedSegment, decompressedLength, _end);
                if (_begin == null)
                    _begin = _end;

                position += compressedChunkLength;
            }
        }

        public void Dispose()
        {
            var cur = _begin;
            while (cur != null)
            {
                ArrayPool<byte>.Shared.Return(cur.LeasedMemory);
                cur = (SequenceNode)cur.Next;
            }
        }

        public ReadOnlySequence<byte> Sequence => new(_begin, 0, _end, _end.Memory.Length);
    }

    sealed class SequenceNode : ReadOnlySequenceSegment<byte>
    {
        public byte[] LeasedMemory { get; private set; }

        internal SequenceNode(byte[] leasedMemory, int length, SequenceNode previous)
        {
            Memory = leasedMemory.AsMemory(0, length);
            if (previous != null)
            {
                previous.Next = this;
                RunningIndex = previous.RunningIndex + previous.Memory.Length;
            }
        }
    }
}
