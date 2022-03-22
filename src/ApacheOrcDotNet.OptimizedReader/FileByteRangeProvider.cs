using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader
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

        public void GetRange(Span<byte> buffer, int position)
        {
            _stream.Seek(position, SeekOrigin.Begin);
            int start = 0;
            int remaining = buffer.Length;
            while (remaining > 0)
            {
                int count = _stream.Read(buffer[start..]);
                start += count;
                remaining -= count;
            }
        }
    }
}
