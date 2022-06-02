using ApacheOrcDotNet.Protocol;
using System;

namespace ApacheOrcDotNet.OptimizedReader
{
    public class OrcColumn
    {
        public OrcColumn(int id, string name, ColumnTypeKind type)
        {
            Id = id;
            Name = name;
            Type = type;
        }

        public int Id { get; }
        public string Name { get; }
        public ColumnTypeKind Type { get; }

        public string Min { get; init; }
        public string Max { get; init; }

        public override bool Equals(object obj)
        {
            if (obj is OrcColumn)
            {
                var other = obj as OrcColumn;
                return Id == other.Id && Name == other.Name && Type == other.Type;
            }

            return false;
        }

        public override int GetHashCode() => HashCode.Combine(Id, Name, Type);
    }
}
