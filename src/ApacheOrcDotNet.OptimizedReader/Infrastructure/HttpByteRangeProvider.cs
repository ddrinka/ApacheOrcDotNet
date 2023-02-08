using System;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public sealed class HttpByteRangeProvider : IByteRangeProvider
    {
        readonly HttpClient _httpClient = new();
        readonly string _remoteLocation;

        internal HttpByteRangeProvider(string remoteLocation)
        {
            _remoteLocation = remoteLocation;
        }

        public void Dispose() => _httpClient.Dispose();

        public void GetRange(Span<byte> buffer, long position)
        {
            var request = CreateRangeRequest(position, position + buffer.Length);
            var response = _httpClient.Send(request);

            if (!response.Content.Headers.ContentRange.Length.HasValue)
                throw new InvalidRangeResponseException();

            var bytesRead = DoRead(response.Content.ReadAsStream(), buffer);
            if (bytesRead < buffer.Length)
                throw new BufferNotFilledException();
        }

        public async Task GetRangeAsync(Memory<byte> buffer, long position)
        {
            var request = CreateRangeRequest(position, position + buffer.Length);
            var response = await _httpClient.SendAsync(request);

            if (!response.Content.Headers.ContentRange.Length.HasValue)
                throw new InvalidRangeResponseException();

            var httpStream = await response.Content.ReadAsStreamAsync();

            var bytesRead = await DoReadAsync(httpStream, buffer);
            if (bytesRead < buffer.Length)
                throw new BufferNotFilledException();
        }

        public void GetRangeFromEnd(Span<byte> buffer)
        {
            var request = CreateRangeRequest(null, buffer.Length);
            var response = _httpClient.Send(request);

            if (!response.Content.Headers.ContentRange.Length.HasValue)
                throw new InvalidRangeResponseException();

            var bytesRead = DoRead(response.Content.ReadAsStream(), buffer);
            if (bytesRead < buffer.Length)
                throw new BufferNotFilledException();
        }

        public async Task GetRangeFromEndAsync(Memory<byte> buffer)
        {
            var request = CreateRangeRequest(null, buffer.Length);
            var response = await _httpClient.SendAsync(request);

            if (!response.Content.Headers.ContentRange.Length.HasValue)
                throw new InvalidRangeResponseException();

            var httpStream = await response.Content.ReadAsStreamAsync();

            var bytesRead = await DoReadAsync(httpStream, buffer);
            if (bytesRead < buffer.Length)
                throw new BufferNotFilledException();
        }

        private HttpRequestMessage CreateRangeRequest(long? from, long? to)
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

        private async Task<int> DoReadAsync(Stream stream, Memory<byte> buffer)
        {
            int bytesRead = 0;
            int bytesRemaining = buffer.Length;

            while (bytesRemaining > 0)
            {
                int count = await stream.ReadAsync(buffer[bytesRead..]);
                if (count == 0)
                    break;

                bytesRead += count;
                bytesRemaining -= count;
            }

            return bytesRead;
        }
    }
}
