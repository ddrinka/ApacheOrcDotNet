﻿using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;

namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public sealed class AmazonS3ByteRangeProvider : IByteRangeProvider
    {
        readonly ConcurrentDictionary<long, byte[]> _cache = new();
        readonly HttpClient _httpClient = new();
        readonly string _remoteLocation;
        readonly long _length;

        internal AmazonS3ByteRangeProvider(string remoteLocation)
        {
            _remoteLocation = remoteLocation;
            _length = GetLength();
        }

        public void Dispose() => _httpClient.Dispose();

        public int GetRange(Span<byte> buffer, long position)
        {
            var key = position;
            if (!_cache.TryGetValue(key, out var bytes))
            {
                var request = CreateRangeRequest(position, position + buffer.Length);
                var response = _httpClient.Send(request);

                if (!response.Content.Headers.ContentRange.Length.HasValue)
                    throw new InvalidOperationException("Range respose must include a length.");

                DoRead(response.Content.ReadAsStream(), buffer);

                bytes = buffer.ToArray();

                _cache.TryAdd(key, bytes);
            }

            return bytes.Length;
        }

        public int GetRangeFromEnd(Span<byte> buffer, long positionFromEnd)
        {
            var key = positionFromEnd;
            if (!_cache.TryGetValue(key, out var bytes))
            {
                var request = CreateRangeRequest(_length - positionFromEnd, (_length - positionFromEnd) + buffer.Length);
                var response = _httpClient.Send(request);

                if (!response.Content.Headers.ContentRange.Length.HasValue)
                    throw new InvalidOperationException("Range respose must include a length.");

                DoRead(response.Content.ReadAsStream(), buffer);

                bytes = buffer.ToArray();

                _cache.TryAdd(key, bytes);
            }

            return bytes.Length;
        }

        private long GetLength()
        {
            var request = new HttpRequestMessage(HttpMethod.Head, _remoteLocation);
            var response = _httpClient.Send(request);

            if (!response.Content.Headers.ContentLength.HasValue)
                throw new InvalidOperationException("Remote resouce length is required.");

            return response.Content.Headers.ContentLength.Value;
        }

        private HttpRequestMessage CreateRangeRequest(long from, long to)
        {
            var request = new HttpRequestMessage(HttpMethod.Get, _remoteLocation);
            var rangeHeader = new RangeHeaderValue(from, to);
            request.Headers.Range = rangeHeader;
            return request;
        }

        private int DoRead(Stream stream, Span<byte> buffer)
        {
            int bytesRead = 0;
            int bytesRemaining = buffer.Length;
            while (bytesRemaining > 0)
            {
                int count = stream.Read(buffer[bytesRead..]);
                if (count == 0)
                    break;

                bytesRead += count;
                bytesRemaining -= count;
            }
            return bytesRead;
        }
    }
}
