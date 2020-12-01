using ProtoBuf.Meta;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.Infrastructure
{
    public static class StaticProtoBuf
    {
		static StaticProtoBuf()
		{
			Serializer = RuntimeTypeModel.Create();
			Serializer.UseImplicitZeroDefaults = false;
		}

		public static RuntimeTypeModel Serializer { get; }
    }
}
