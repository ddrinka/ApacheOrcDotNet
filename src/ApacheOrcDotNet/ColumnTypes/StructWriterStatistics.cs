
/* Unmerged change from project 'ApacheOrcDotNet (net50)'
Before:
using System;
After:
using ApacheOrcDotNet.Statistics;
using System;
*/
using 
/* Unmerged change from project 'ApacheOrcDotNet (net50)'
Before:
using System.Threading.Tasks;
using ApacheOrcDotNet.Statistics;
After:
using System.Threading.Tasks;
*/
ApacheOrcDotNet.Statistics;

namespace ApacheOrcDotNet.ColumnTypes {
    public class StructWriterStatistics : ColumnWriterStatistics, IStatistics {
        public ulong NumValues { get; set; } = 0;

        public void FillColumnStatistics(ColumnStatistics columnStatistics) {
            columnStatistics.NumberOfValues += NumValues;
        }
    }
}
