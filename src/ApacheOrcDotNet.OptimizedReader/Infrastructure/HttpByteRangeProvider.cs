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

        public void FillBuffer(Span<byte> buffer, long position)
        {
            var request = CreateRangeRequest(position, position + buffer.Length);
            var response = _httpClient.Send(request);

            if (!response.Content.Headers.ContentRange.Length.HasValue)
                throw new InvalidRangeResponseException();

            var bytesRead = response.Content.ReadAsStream().Read(buffer);
            if (bytesRead < buffer.Length)
                throw new BufferNotFilledException();
        }

        public async Task FillBufferAsync(Memory<byte> buffer, long position)
        {
            var request = CreateRangeRequest(position, position + buffer.Length);
            var response = await _httpClient.SendAsync(request);

            if (!response.Content.Headers.ContentRange.Length.HasValue)
                throw new InvalidRangeResponseException();

            var httpStream = await response.Content.ReadAsStreamAsync();

            var bytesRead = await httpStream.ReadAsync(buffer);
            if (bytesRead < buffer.Length)
                throw new BufferNotFilledException();
        }

        public void FillBufferFromEnd(Span<byte> buffer)
        {
            var request = CreateRangeRequest(null, buffer.Length);
            var response = _httpClient.Send(request);

            if (!response.Content.Headers.ContentRange.Length.HasValue)
                throw new InvalidRangeResponseException();

            var bytesRead = response.Content.ReadAsStream().Read(buffer);
            if (bytesRead < buffer.Length)
                throw new BufferNotFilledException();
        }

        public async Task FillBufferFromEndAsync(Memory<byte> buffer)
        {
            var request = CreateRangeRequest(null, buffer.Length);
            var response = await _httpClient.SendAsync(request);

            if (!response.Content.Headers.ContentRange.Length.HasValue)
                throw new InvalidRangeResponseException();

            var bytesRead = await (await response.Content.ReadAsStreamAsync()).ReadAsync(buffer);
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
    }
}
