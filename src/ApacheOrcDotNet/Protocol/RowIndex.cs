using ProtoBuf;
using System.Collections.Generic;

namespace ApacheOrcDotNet.Protocol {
    [ProtoContract]
    public class RowIndex {
        [ProtoMember(1)]
        public List<RowIndexEntry> Entry { get; } = new List<RowIndexEntry>();
    }
}
