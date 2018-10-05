namespace ApacheOrcDotNet.FluentSerialization
{
	public class SerializationPropertyConfiguration
    {
		public bool ExcludeFromSerialization { get; set; }
		public bool SerializeAsDate { get; set; }
		public int DecimalPrecision { get; set; }
		public int DecimalScale { get; set; }
	}
}
