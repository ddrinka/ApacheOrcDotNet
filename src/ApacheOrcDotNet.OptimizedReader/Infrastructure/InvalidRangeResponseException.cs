using System;

namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public class InvalidRangeResponseException : Exception
    {
        public InvalidRangeResponseException() : base("Range response must include a length.")
        {
        }
    }
}
