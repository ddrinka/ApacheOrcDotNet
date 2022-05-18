using ApacheOrcDotNet.Protocol;

namespace ApacheOrcDotNet.OptimizedReader
{
    public record ColumnDetail(int ColumnId, string Name, ColumnTypeKind ColumnType);
}
