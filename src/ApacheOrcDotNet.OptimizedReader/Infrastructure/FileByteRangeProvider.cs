using System;
using System.IO;

namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public sealed class FileByteRangeProvider : IByteRangeProvider
    {
        readonly FileStream _stream;

        internal FileByteRangeProvider(string path)
        {
            _stream = File.OpenRead(path);
        }

        public void Dispose()
        {
            _stream.Dispose();
        }

        public int GetRange(Span<byte> buffer, long position)
        {
            _stream.Seek(position, SeekOrigin.Begin);
            return DoRead(buffer);
        }

        public int GetRangeFromEnd(Span<byte> buffer, long positionFromEnd)
        {
            _stream.Seek(-positionFromEnd, SeekOrigin.End);
            return DoRead(buffer);
        }

        private int DoRead(Span<byte> buffer)
        {
            int bytesRead = 0;
            int bytesRemaining = buffer.Length;
            while (bytesRemaining > 0)
            {
                int count = _stream.Read(buffer[bytesRead..]);
                if (count == 0)
                    break;

                bytesRead += count;
                bytesRemaining -= count;
            }
            return bytesRead;
        }
    }
}
