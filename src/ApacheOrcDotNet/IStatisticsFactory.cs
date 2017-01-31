using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ApacheOrcDotNet
{
    public interface IStatisticsFactory<T>
    {
		IStatistics<T> CreateStatistics();
    }
}
