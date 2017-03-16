using ApacheOrcDotNet.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace ApacheOrcDotNet.Test.Infrastructure
{
    public class DecimalExtensions_Test
    {
		public void ToLongAndScale_ScaleOf0()
		{
			var result = 100m.ToLongAndScale();
			Assert.Equal(100, result.Item1);
			Assert.Equal(0, result.Item2);
		}

		public void ToLongAndScale_ScaleNotNormalized()
		{
			var result = 100.0m.ToLongAndScale();
			Assert.Equal(1000, result.Item1);
			Assert.Equal(1, result.Item2);
		}

		public void ToLongAndScale_ScaleNormalized()
		{
			var result = 100.5m.ToLongAndScale();
			Assert.Equal(1005, result.Item1);
			Assert.Equal(1, result.Item2);
		}

		public void ToLongAndScale_Negative()
		{
			var result = (-100m).ToLongAndScale();
			Assert.Equal(-100, result.Item1);
			Assert.Equal(0, result.Item2);
		}

		public void ToLongAndScale_MoreThan32Bits()
		{
			var result = 68719476735m.ToLongAndScale();
			Assert.Equal(68719476735, result.Item1);
			Assert.Equal(0, result.Item2);
		}

		public void ToLongAndScale_64Bits_ShouldThrow()
		{
			var dec = new decimal(ulong.MaxValue);
			try
			{
				var result = dec.ToLongAndScale();
				Assert.True(false, "Should have thrown");
			}
			catch (OverflowException)
			{ }
		}

		public void ToLongAndScale_MoreThan64Bits_ShouldThrow()
		{
			var dec = new decimal(ulong.MaxValue);
			dec += 100;
			try
			{
				var result = dec.ToLongAndScale();
				Assert.True(false, "Should have thrown");
			}
			catch (OverflowException)
			{ }
		}

		public void ToDecimal_Positive()
		{
			var result = 100m.ToLongAndScale().ToDecimal();
			Assert.Equal(100m, result);
		}

		public void ToDecimal_Negative()
		{
			var result = (-100m).ToLongAndScale().ToDecimal();
			Assert.Equal(-100m, result);
		}

		public void Rescale_NoScalingNeeded()
		{
			var tuple = 100.5m.ToLongAndScale();
			var result = tuple.Rescale(1, false);
			Assert.Equal(1005, result.Item1);
			Assert.Equal(1, result.Item2);
		}

		public void Rescale_Upscale()
		{
			var tuple = 100.5m.ToLongAndScale();
			var result = tuple.Rescale(2, false);
			Assert.Equal(10050, result.Item1);
			Assert.Equal(2, result.Item2);
		}

		public void Rescale_UpscaleOverflow_ShouldThrow()
		{
			var tuple = 100000000000.5m.ToLongAndScale();
			try
			{
				var result = tuple.Rescale(8, false);
				Assert.True(false, "Should have thrown");
			}
			catch (OverflowException)
			{ }
		}

		public void Rescale_Downscale()
		{
			var tuple = 100.0m.ToLongAndScale();
			var result = tuple.Rescale(0, false);
			Assert.Equal(100, result.Item1);
			Assert.Equal(0, result.Item2);
		}

		public void Rescale_Downscale_TruncatingDisabled_ShouldThrow()
		{
			var tuple = 100.5m.ToLongAndScale();
			try
			{
				var result = tuple.Rescale(0, false);
				Assert.True(false, "Should have thrown");
			}
			catch(ArithmeticException)
			{ }
		}

		public void Rescale_Downscale_TruncatingEnabled()
		{
			var tuple = 100.5m.ToLongAndScale();
			var result = tuple.Rescale(0, true);
			Assert.Equal(100, result.Item1);
			Assert.Equal(0, result.Item2);
		}
    }
}
