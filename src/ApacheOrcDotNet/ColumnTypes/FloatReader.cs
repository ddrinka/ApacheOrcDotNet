using ApacheOrcDotNet.Encodings;
using ApacheOrcDotNet.Stripes;
using System.Collections.Generic;

namespace ApacheOrcDotNet.ColumnTypes {
    public class FloatReader : ColumnReader {
        public FloatReader(StripeStreamReaderCollection stripeStreams, uint columnId) : base(stripeStreams, columnId) {
        }

        public IEnumerable<float?> Read() {
            var present = ReadBooleanStream(Protocol.StreamKind.Present);
            var data = ReadBinaryStream(Protocol.StreamKind.Data);
            int dataIndex = 0;
            if (present == null) {
                while (dataIndex + 4 <= data.Length) {
                    var value = BitManipulation.ReadFloat(data, dataIndex);
                    dataIndex += 4;
                    yield return value;
                }
            } else {
                foreach (var isPresent in present) {
                    if (isPresent) {
                        var value = BitManipulation.ReadFloat(data, dataIndex);
                        dataIndex += 4;
                        yield return value;
                    } else
                        yield return null;
                }
            }
        }
    }
}
