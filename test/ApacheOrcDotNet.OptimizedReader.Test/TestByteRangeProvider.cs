using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Test.TestHelpers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader
{
    public class TestByteRangeProvider : IByteRangeProvider
    {
        readonly bool _writeRequestedRangesToFile;
        readonly bool _readRequestedRangesFromFile;
        readonly Dictionary<(bool isFromEnd, long position, int length), Stream> _openFiles = new();
        readonly static object _fileLock = new();

        public TestByteRangeProvider(bool writeRequestedRangesToFile, bool readRequestedRangesFromFile)
        {
            _writeRequestedRangesToFile = writeRequestedRangesToFile;
            _readRequestedRangesFromFile = readRequestedRangesFromFile;

            if (_writeRequestedRangesToFile == true && _readRequestedRangesFromFile == true)
                throw new InvalidOperationException("Cannot read and write to range files simultaneously");
        }

        public void Dispose()
        {
            foreach (var file in _openFiles.Values)
                file.Dispose();
        }

        public void FillBuffer(Span<byte> buffer, long position)
        {
            var reader = GetOpenStreamForRange(false, position, buffer.Length);

            if (!_readRequestedRangesFromFile)
                reader.Seek(position, SeekOrigin.Begin);

            var bytesRead = reader.Read(buffer);
            if (bytesRead < buffer.Length)
                throw new InvalidOperationException("Insufficient data to fill the buffer.");

            if (_writeRequestedRangesToFile)
            {
                var filename = GetRangeFilename(false, position, buffer.Length);
                var path = Path.Combine(Path.GetTempPath(), filename);
                lock (_fileLock)
                {
                    using var outputStream = File.Create(path);
                    outputStream.Write(buffer);
                }
            }
        }

        public Task FillBufferAsync(Memory<byte> buffer, long position)
        {
            throw new NotImplementedException();
        }

        public void FillBufferFromEnd(Span<byte> buffer)
        {
            var reader = GetOpenStreamForRange(true, buffer.Length, buffer.Length);

            if (!_readRequestedRangesFromFile)
                reader.Seek(-buffer.Length, SeekOrigin.End);

            var bytesRead = reader.Read(buffer);
            if (bytesRead < buffer.Length)
                throw new InvalidOperationException("Insufficient data to fill the buffer.");

            if (_writeRequestedRangesToFile)
            {
                var filename = GetRangeFilename(true, buffer.Length, buffer.Length);
                var path = Path.Combine(Path.GetTempPath(), filename);
                lock (_fileLock)
                {
                    using var outputStream = File.Create(path);
                    outputStream.Write(buffer);
                }
            }
        }

        public Task FillBufferFromEndAsync(Memory<byte> buffer)
        {
            throw new NotImplementedException();
        }

        Stream GetOpenStreamForRange(bool isFromEnd, long position, int length)
        {
            if (_openFiles.TryGetValue((isFromEnd, position, length), out var existingStream))
                return existingStream;

            lock (_fileLock)
            {
                if (_openFiles.TryGetValue((isFromEnd, position, length), out var existingStreamSecondTry))
                    return existingStreamSecondTry;

                Stream newStream;
                if (_readRequestedRangesFromFile)
                {
                    var filename = GetRangeFilename(isFromEnd, position, length);
                    var helper = new DataFileHelper(typeof(TestByteRangeProvider), filename);
                    newStream = helper.GetStream();
                }
                else
                {
                    var filename = "/data/2022-03-18/trade.orc";    //TODO remove this when all test ranges are loadable from small files
                    newStream = File.OpenRead(filename);
                }

                _openFiles.Add((isFromEnd, position, length), newStream);

                return newStream;
            }
        }

        string GetRangeFilename(bool isFromEnd, long position, int length)
        {
            string fromEnd = isFromEnd ? "fromEnd" : "fromStart";
            return $"orctest_{fromEnd}_{position}_{length}.orc";
        }
    }
}
