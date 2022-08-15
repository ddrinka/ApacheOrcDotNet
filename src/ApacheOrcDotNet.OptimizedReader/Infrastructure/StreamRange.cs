namespace ApacheOrcDotNet.OptimizedReader.Infrastructure
{
    public record StreamRange(int stripeId, long Offset, int Length);
}
