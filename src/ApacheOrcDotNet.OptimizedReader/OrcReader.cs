using ApacheOrcDotNet.Protocol;
using ApacheOrcDotNet.Statistics;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApacheOrcDotNet.OptimizedReader
{
    public class ColumnDescriptor
    {
        public string ColumnName { get; set; }
        public string Min { get; set; }
        public string Max { get; set; }
    }

    public class OrcReaderConfiguration
    {
        public int OptimisticFileTailReadLength { get; set; } = 16 * 1024;
    }

    public sealed class OrcReader
    {
        readonly OrcReaderConfiguration _configuration;
        readonly IByteRangeProvider _byteRangeProvider;
        readonly SpanFileTail _fileTail;
        readonly Dictionary<int, RowIndex> _stripeIndices = new();

        public OrcReader(OrcReaderConfiguration configuration, IByteRangeProvider byteRangeProvider)
        {
            _configuration = configuration;
            _byteRangeProvider = byteRangeProvider;

            _fileTail = ReadFileTail();
        }

        public IEnumerable<OrcReaderResultSet<T>> Search<T>(params ColumnDescriptor[] columns) where T : struct
        {
            throw new NotImplementedException();
        }

        bool FileContainsData(params ColumnDescriptor[] columns)
        {
            foreach (var descriptor in columns)
            {
                var (columnId, columnType) = LookUpColumn(descriptor);
                var stats = _fileTail.Footer.Statistics[columnId];
                if (!stats.InRange(columnType, descriptor.Min, descriptor.Max))
                    return false;
            }
            return true;
        }

        (int columnId, ColumnTypeKind columnType) LookUpColumn(ColumnDescriptor descriptor)
        {
            var columnId = _fileTail.Footer.Types[0].FieldNames.FindIndex(fn => fn.ToLower() == descriptor.ColumnName.ToLower()) + 1;
            if (columnId == -1)
                throw new KeyNotFoundException($"'{descriptor.ColumnName}' not found in ORC data");
            var columnType = _fileTail.Footer.Types[columnId].Kind;

            return (columnId, columnType);
        }

        SpanFileTail ReadFileTail()
        {
            int lengthToReadFromEnd = _configuration.OptimisticFileTailReadLength;
            while (true)
            {
                var buffer = ArrayPool<byte>.Shared.Rent(lengthToReadFromEnd);
                _byteRangeProvider.GetRangeFromEnd(buffer, lengthToReadFromEnd);
                var success = SpanFileTail.TryRead(buffer, out var fileTail, out var additionalBytesRequired);
                ArrayPool<byte>.Shared.Return(buffer);

                if (success)
                    return fileTail;

                lengthToReadFromEnd += additionalBytesRequired;
            }
        }

        void EnsureStripeIndexRead(int stripeId)
        {
            if (_stripeIndices.ContainsKey(stripeId))
                return;

            var stripe = _fileTail.Footer.Stripes[stripeId];
            var stripeIndexStart = stripe.Offset;
            var stripIndexLength = stripe.IndexLength;
        }
    }
}
