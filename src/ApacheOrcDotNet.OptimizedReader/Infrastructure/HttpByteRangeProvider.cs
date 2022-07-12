using System;
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
                throw new InvalidOperationException("Range response must include a length.");

            return response.Content.ReadAsStream().Read(buffer);
        }

        public async Task<int> GetRangeAsync(Memory<byte> buffer, long position)
        {
            var request = CreateRangeRequest(position, position + buffer.Length);
            var response = await _httpClient.SendAsync(request);

            if (!response.Content.Headers.ContentRange.Length.HasValue)
                throw new InvalidOperationException("Range response must include a length.");

            return await (await response.Content.ReadAsStreamAsync()).ReadAsync(buffer);
        }

        public int GetRangeFromEnd(Span<byte> buffer)
        {
            var request = CreateRangeRequest(null, buffer.Length);
            var response = _httpClient.Send(request);

            if (!response.Content.Headers.ContentRange.Length.HasValue)
                throw new InvalidOperationException("Range response must include a length.");

            return response.Content.ReadAsStream().Read(buffer);
        }

        public async Task<int> GetRangeFromEndAsync(Memory<byte> buffer)
        {
            var request = CreateRangeRequest(null, buffer.Length);
            var response = await _httpClient.SendAsync(request);

            if (!response.Content.Headers.ContentRange.Length.HasValue)
                throw new InvalidOperationException("Range response must include a length.");

            return await (await response.Content.ReadAsStreamAsync()).ReadAsync(buffer);
        }

        private HttpRequestMessage CreateRangeRequest(long? from, long? to)
        {
            var request = new HttpRequestMessage(HttpMethod.Get, _remoteLocation);
            var rangeHeader = new RangeHeaderValue(from, to);
            request.Headers.Range = rangeHeader;
            return request;
        }
    }
}
