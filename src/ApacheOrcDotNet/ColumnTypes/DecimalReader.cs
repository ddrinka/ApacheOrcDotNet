using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.ColumnTypes
{
	public class DecimalReader : ColumnReader
	{
		public DecimalReader(StripeStreamCollection stripeStreams, uint columnId) : base(stripeStreams, columnId)
		{
		}

		public IEnumerable<decimal?> Read()
		{
			var present = ReadBooleanStream(Protocol.StreamKind.Present);
			var data = ReadVarIntStream(Protocol.StreamKind.Data);
			var secondary = ReadNumericStream(Protocol.StreamKind.Secondary, false);
			if(data==null||secondary==null)
				throw new InvalidDataException("DATA and SECONDARY streams must be available");

			var dataEnumerator = ((IEnumerable<BigInteger>)data).GetEnumerator();
			var secondaryEnumerator = ((IEnumerable<long>)secondary).GetEnumerator();
			if (present == null)
			{
				while(dataEnumerator.MoveNext() && secondaryEnumerator.MoveNext())
				{
					var value = FromBigInteger(dataEnumerator.Current, secondaryEnumerator.Current);
					yield return value;
				}
			}
			else
			{
				foreach (var isPresent in present)
				{
					if (isPresent)
					{
						var success = dataEnumerator.MoveNext() && secondaryEnumerator.MoveNext();
						if (!success)
							throw new InvalidDataException("The PRESENT data stream's length didn't match the DATA and SECONDARY streams' lengths");

						var value = FromBigInteger(dataEnumerator.Current, secondaryEnumerator.Current);
						yield return value;
					}
					else
						yield return null;
				}
			}
		}

		decimal FromBigInteger(BigInteger numerator, long scale)
		{
			var denominator = BigInteger.One;
			while (scale-- != 0)
				denominator *= 10;
			var value = numerator / denominator;
			return (decimal)value;
		}
	}
}
