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

        public int GetRange(Span<byte> buffer, long position)
        {
            var request = CreateRangeRequest(position, position + buffer.Length);
            var response = _httpClient.Send(request);

            if (!response.Content.Headers.ContentRange.Length.HasValue)
                throw new InvalidOperationException("Range respose must include a length.");

            return DoRead(response.Content.ReadAsStream(), buffer);
        }

        public async Task<int> GetRangeAsync(Memory<byte> buffer, long position)
        {
            var request = CreateRangeRequest(position, position + buffer.Length);
            var response = await _httpClient.SendAsync(request);

            if (!response.Content.Headers.ContentRange.Length.HasValue)
                throw new InvalidOperationException("Range respose must include a length.");

            return await DoReadAsync(await response.Content.ReadAsStreamAsync(), buffer);
        }

        public int GetRangeFromEnd(Span<byte> buffer, long positionFromEnd)
        {
            var request = CreateRangeRequest(null, positionFromEnd);
            var response = _httpClient.Send(request);

            if (!response.Content.Headers.ContentRange.Length.HasValue)
                throw new InvalidOperationException("Range respose must include a length.");

            return DoRead(response.Content.ReadAsStream(), buffer);
        }

        public async Task<int> GetRangeFromEndAsync(Memory<byte> buffer, long positionFromEnd)
        {
            var request = CreateRangeRequest(null, positionFromEnd);
            var response = await _httpClient.SendAsync(request);

            if (!response.Content.Headers.ContentRange.Length.HasValue)
                throw new InvalidOperationException("Range respose must include a length.");

            return await DoReadAsync(await response.Content.ReadAsStreamAsync(), buffer);
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
