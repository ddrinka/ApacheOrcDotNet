using System;

namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public class CompressionBufferException : ArgumentException
    {
        public CompressionBufferException(string parameterName, long bufferCurrentLength, long bufferRequiredLength)
            : base($"Buffer too small. Current length is '{bufferCurrentLength}' bytes, but '{bufferRequiredLength}' bytes are required. Please increase the length of buffers in '{nameof(OrcReaderConfiguration)}'.", parameterName)
        {
        }
    }
}
