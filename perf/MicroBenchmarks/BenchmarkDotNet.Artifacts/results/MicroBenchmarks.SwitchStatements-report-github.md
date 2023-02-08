``` ini

BenchmarkDotNet=v0.13.1, OS=Windows 10.0.19044.1706 (21H2)
Intel Core i7-10750H CPU 2.60GHz, 1 CPU, 12 logical and 6 physical cores
.NET SDK=6.0.300
  [Host]     : .NET 5.0.17 (5.0.1722.21314), X64 RyuJIT
  DefaultJob : .NET 5.0.17 (5.0.1722.21314), X64 RyuJIT


```
|          Method |     Mean |     Error |    StdDev | Rank | Allocated |
|---------------- |---------:|----------:|----------:|-----:|----------:|
|        Standard | 1.770 ns | 0.0102 ns | 0.0090 ns |    2 |         - |
| PatternMatching | 1.688 ns | 0.0584 ns | 0.0738 ns |    1 |         - |
