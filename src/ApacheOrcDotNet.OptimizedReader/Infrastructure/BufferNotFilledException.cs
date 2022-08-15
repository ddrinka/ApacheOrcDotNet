using System;

namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public class BufferNotFilledException : Exception
    {
        public BufferNotFilledException() : base("Insufficient data to fill the buffer.")
        {
        }
    }
}
