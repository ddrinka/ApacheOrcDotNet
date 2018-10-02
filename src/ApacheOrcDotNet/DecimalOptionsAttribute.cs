using System;
using System.Collections.Generic;
using System.Text;

namespace ApacheOrcDotNet
{
	[AttributeUsage(AttributeTargets.Property)]
    public class DecimalOptionsAttribute : Attribute
    {
		public int Precision { get; set; }
		public int Scale { get; set; }

		public DecimalOptionsAttribute() { }
		public DecimalOptionsAttribute(int precision, int scale)
		{
			Precision = precision;
			Scale = scale;
		}
	}
}
