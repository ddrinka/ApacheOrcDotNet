using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Test.TestHelpers;
using System;
using System.IO;

namespace ApacheOrcDotNet.OptimizedReader
{
    public sealed class TestByteRangeProviderParallel : IByteRangeProvider
    {
        readonly object _syncRoot = new object();
        readonly Stream _stream;

        internal TestByteRangeProviderParallel()
        {
            var dataFileHelper = new DataFileHelper(typeof(TestByteRangeProvider), "optimized_reader_test_file.orc");
            _stream = dataFileHelper.GetStream();
        }

        public void Dispose()
        {
            _stream.Dispose();
        }

        public int GetRange(Span<byte> buffer, long position)
        {
            lock (_syncRoot)
            {
                _stream.Seek(position, SeekOrigin.Begin);
                return DoRead(buffer);
            }
        }

        public int GetRangeFromEnd(Span<byte> buffer, long positionFromEnd)
        {
            lock (_syncRoot)
            {
                _stream.Seek(-positionFromEnd, SeekOrigin.End);
                return DoRead(buffer);
            }
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
