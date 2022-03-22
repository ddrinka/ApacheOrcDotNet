using ApacheOrcDotNet.Protocol;
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
        SpanFileTail _fileTail = null;

        public OrcReader(OrcReaderConfiguration configuration, IByteRangeProvider byteRangeProvider)
        {
            _configuration = configuration;
            _byteRangeProvider = byteRangeProvider;
        }

        public IEnumerable<OrcReaderResultSet<T>> Search<T>(params ColumnDescriptor[] columns) where T : struct
        {
            throw new NotImplementedException();
        }

        void EnsureFileTailRead()
        {
            if (_fileTail != null)
                return;

            int lengthToReadFromEnd = _configuration.OptimisticFileTailReadLength;
            while (true)
            {
                var buffer = ArrayPool<byte>.Shared.Rent(lengthToReadFromEnd);
                _byteRangeProvider.GetRangeFromEnd(buffer, lengthToReadFromEnd);
                var success = SpanFileTail.TryRead(buffer, out _fileTail, out var additionalBytesRequired);
                ArrayPool<byte>.Shared.Return(buffer);

                if (success)
                    break;

                lengthToReadFromEnd += additionalBytesRequired;
            }
        }
    }
}
