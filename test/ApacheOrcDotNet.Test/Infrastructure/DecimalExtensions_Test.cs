using ApacheOrcDotNet.Infrastructure;
using System;
using Xunit;

namespace ApacheOrcDotNet.Test.Infrastructure {
    public class DecimalExtensions_Test {
        [Fact]
        public void ToLongAndScale_ScaleOf0() {
            var result = 100m.ToLongAndScale();
            Assert.Equal(100, result.Item1);
            Assert.Equal(0, result.Item2);
        }

        [Fact]
        public void ToLongAndScale_ScaleNotNormalized() {
            var result = 100.0m.ToLongAndScale();
            Assert.Equal(1000, result.Item1);
            Assert.Equal(1, result.Item2);
        }

        [Fact]
        public void ToLongAndScale_ScaleNormalized() {
            var result = 100.5m.ToLongAndScale();
            Assert.Equal(1005, result.Item1);
            Assert.Equal(1, result.Item2);
        }

        [Fact]
        public void ToLongAndScale_Negative() {
            var result = (-100m).ToLongAndScale();
            Assert.Equal(-100, result.Item1);
            Assert.Equal(0, result.Item2);
        }

        [Fact]
        public void ToLongAndScale_MoreThan32Bits() {
            var result = 68719476735m.ToLongAndScale();
            Assert.Equal(68719476735, result.Item1);
            Assert.Equal(0, result.Item2);
        }

        [Fact]
        public void ToLongAndScale_64Bits_ShouldThrow() {
            var dec = new decimal(ulong.MaxValue);
            try {
                var result = dec.ToLongAndScale();
                Assert.True(false, "Should have thrown");
            } catch (OverflowException) { }
        }

        [Fact]
        public void ToLongAndScale_MoreThan64Bits_ShouldThrow() {
            var dec = new decimal(ulong.MaxValue);
            dec += 100;
            try {
                var result = dec.ToLongAndScale();
                Assert.True(false, "Should have thrown");
            } catch (OverflowException) { }
        }

        [Fact]
        public void ToDecimal_Positive() {
            var result = 100m.ToLongAndScale().ToDecimal();
            Assert.Equal(100m, result);
        }

        [Fact]
        public void ToDecimal_Negative() {
            var result = (-100m).ToLongAndScale().ToDecimal();
            Assert.Equal(-100m, result);
        }

        [Fact]
        public void Rescale_NoScalingNeeded() {
            var tuple = 100.5m.ToLongAndScale();
            var result = tuple.Rescale(1, false);
            Assert.Equal(1005, result.Item1);
            Assert.Equal(1, result.Item2);
        }

        [Fact]
        public void Rescale_Upscale() {
            var tuple = 100.5m.ToLongAndScale();
            var result = tuple.Rescale(2, false);
            Assert.Equal(10050, result.Item1);
            Assert.Equal(2, result.Item2);
        }

        [Fact]
        public void Rescale_UpscaleOverflow_ShouldThrow() {
            var tuple = 100000000000.5m.ToLongAndScale();
            try {
                var result = tuple.Rescale(8, false);
                Assert.True(false, "Should have thrown");
            } catch (OverflowException) { }
        }

        [Fact]
        public void Rescale_Downscale() {
            var tuple = 100.0m.ToLongAndScale();
            var result = tuple.Rescale(0, false);
            Assert.Equal(100, result.Item1);
            Assert.Equal(0, result.Item2);
        }

        [Fact]
        public void Rescale_Downscale_TruncatingDisabled_ShouldThrow() {
            var tuple = 100.5m.ToLongAndScale();
            try {
                var result = tuple.Rescale(0, false);
                Assert.True(false, "Should have thrown");
            } catch (ArithmeticException) { }
        }

        [Fact]
        public void Rescale_Downscale_TruncatingEnabled() {
            var tuple = 100.5m.ToLongAndScale();
            var result = tuple.Rescale(0, true);
            Assert.Equal(100, result.Item1);
            Assert.Equal(0, result.Item2);
        }

        [Fact]
        public void Rescale_Downscale_1() {
            var tuple = 34328.023927m.ToLongAndScale();
            var result = tuple.Rescale(9, truncateIfNecessary: false);
            Assert.Equal(34328023927000, result.Item1);
            Assert.Equal(9, result.Item2);
        }

        [Fact]
        public void Rescale_Downscale_2() {
            var tuple = 34328.02m.ToLongAndScale();
            var result = tuple.Rescale(9, truncateIfNecessary: false);
            Assert.Equal(34328020000000, result.Item1);
            Assert.Equal(9, result.Item2);
        }

        [Fact]
        public void Rescale_Downscale_3() {
            var tuple = 164.657700m.ToLongAndScale();
            var result = tuple.Rescale(4, truncateIfNecessary: false);
            Assert.Equal(1646577, result.Item1);
            Assert.Equal(4, result.Item2);
        }

        [Theory]
        [InlineData(0, 1)]
        [InlineData(1, 1)]
        [InlineData(99, 2)]
        [InlineData(123, 3)]
        [InlineData(1000, 4)]
        [InlineData(99999, 5)]
        [InlineData(1234567890123456789, 19)]
        public void CheckPrecision_Good(long value, int maxPrecision) {
            value.CheckPrecision(maxPrecision);
        }

        [Theory]
        [InlineData(10, 1)]
        [InlineData(11, 1)]
        [InlineData(199, 2)]
        [InlineData(1123, 3)]
        [InlineData(11000, 4)]
        [InlineData(199999, 5)]
        [InlineData(1234567890123456789, 18)]
        public void CheckPrecision_Bad(long value, int maxPrecision) {
            Assert.Throws<OverflowException>(() => {
                value.CheckPrecision(maxPrecision);
            });
        }
    }
}
