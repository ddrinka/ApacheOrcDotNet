using ApacheOrcDotNet.Protocol;

namespace ApacheOrcDotNet.OptimizedReader
{
    public class OrcColumn
    {
        public OrcColumn(int id, int index, string name, ColumnTypeKind type)
        {
            Id = id;
            Index = index;
            Name = name;
            Type = type;
        }

        public int Id { get; }
        public int Index { get; }
        public string Name { get; }
        public ColumnTypeKind Type { get; }

        public string Min { get; init; }
        public string Max { get; init; }
    }
}
