using ApacheOrcDotNet.Test.TestHelpers;
using System;
using System.Collections.Generic;
using System.IO;

namespace ApacheOrcDotNet.OptimizedReader
{
    public class TestByteRangeProvider : IByteRangeProvider
    {
        readonly bool _writeRequestedRangesToFile;
        readonly bool _readRequestedRangesFromFile;
        readonly Dictionary<(bool isFromEnd, int position, int length), Stream> _openFiles = new();
        readonly static object _fileLock = new();

        public TestByteRangeProvider(bool writeRequestedRangesToFile, bool readRequestedRangesFromFile)
        {
            _writeRequestedRangesToFile = writeRequestedRangesToFile;
            _readRequestedRangesFromFile = readRequestedRangesFromFile;
        }

        public void Dispose()
        {
            foreach (var file in _openFiles.Values)
                file.Dispose();
        }

        public void GetRange(Span<byte> buffer, int position)
        {
            var reader = GetOpenStreamForRange(false, position, buffer.Length);
            if (!_readRequestedRangesFromFile)
                reader.Seek(position, SeekOrigin.Begin);
            ReadAllBytes(reader, buffer);
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

        public void GetRangeFromEnd(Span<byte> buffer, int positionFromEnd)
        {
            var reader = GetOpenStreamForRange(true, positionFromEnd, buffer.Length);
            if (!_readRequestedRangesFromFile)
                reader.Seek(-positionFromEnd, SeekOrigin.End);
            ReadAllBytes(reader, buffer);
            if (_writeRequestedRangesToFile)
            {
                var filename = GetRangeFilename(true, positionFromEnd, buffer.Length);
                var path = Path.Combine(Path.GetTempPath(), filename);
                lock (_fileLock)
                {
                    using var outputStream = File.Create(path);
                    outputStream.Write(buffer);
                }
            }
        }

        Stream GetOpenStreamForRange(bool isFromEnd, int position, int length)
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

        string GetRangeFilename(bool isFromEnd, int position, int length)
        {
            string fromEnd = isFromEnd ? "fromEnd" : "fromStart";
            return $"orctest_{fromEnd}_{position}_{length}.orc";
        }

        void ReadAllBytes(Stream stream, Span<byte> buffer)
        {
            var remaining = buffer.Length;
            var pos = 0;
            while (remaining > 0)
            {
                var count = stream.Read(buffer[pos..]);
                if (count == 0)
                    throw new InvalidOperationException();

                remaining -= count;
                pos += count;
            }
        }
    }
}
