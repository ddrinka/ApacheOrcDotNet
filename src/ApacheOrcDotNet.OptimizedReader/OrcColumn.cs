using ApacheOrcDotNet.Protocol;
using System;

namespace ApacheOrcDotNet.OptimizedReader
{
    public class OrcColumn : IEquatable<OrcColumn>
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

        public override bool Equals(object obj)
        {
            if (obj is OrcColumn other)
                return Equals(other);

            return false;
        }

        public bool Equals(OrcColumn other)
            => Id == other.Id && Name == other.Name && Type == other.Type;

        public override int GetHashCode() => HashCode.Combine(Id, Name, Type);
    }
}
