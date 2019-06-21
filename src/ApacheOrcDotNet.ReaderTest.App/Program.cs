using ApacheOrcDotNet.ColumnTypes;
using ApacheOrcDotNet.Protocol;
using System;
using System.IO;
using System.Linq;

namespace ApacheOrcDotNet.ReaderTest.App
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length != 1)
            {
                Console.WriteLine("Usage: <FILENAME>");
                return;
            }

            var filename = args[0];
            var stream = new FileStream(filename, FileMode.Open, FileAccess.Read);
            var fileTail = new FileTail(stream);

            foreach (var stripe in fileTail.Stripes)
            {
                Console.WriteLine($"Reading stripe with {stripe.NumRows} rows");
                var stripeStreamCollection = stripe.GetStripeStreamCollection();

                if (fileTail.Footer.Types[0].Kind != ColumnTypeKind.Struct)
                    throw new InvalidDataException($"The base type must be {nameof(ColumnTypeKind.Struct)}");
                var names = fileTail.Footer.Types[0].FieldNames;

                for (int columnId = 1; columnId < fileTail.Footer.Types.Count; columnId++)
                {
                    var columnType = fileTail.Footer.Types[columnId];
                    var columnName = names[columnId - 1];

                    switch (columnType.Kind)
                    {
                        case ColumnTypeKind.Long:
                        case ColumnTypeKind.Int:
                        case ColumnTypeKind.Short:
                            {
                                Console.WriteLine($"Reading longs from column {columnId} ({columnName})");
                                var reader = new LongReader(stripeStreamCollection, (uint)columnId);
                                var count = reader.Read().Count();
                                Console.WriteLine($"Done reading {count} longs");
                                break;
                            }
                        case ColumnTypeKind.Byte:
                            {
                                Console.WriteLine($"Reading bytes from column {columnId} ({columnName})");
                                var reader = new ByteReader(stripeStreamCollection, (uint)columnId);
                                var count = reader.Read().Count();
                                Console.WriteLine($"Done reading {count} bytes");
                                break;
                            }
                        case ColumnTypeKind.Boolean:
                            {
                                Console.WriteLine($"Reading bools from column {columnId} ({columnName})");
                                var reader = new BooleanReader(stripeStreamCollection, (uint)columnId);
                                var count = reader.Read().Count();
                                Console.WriteLine($"Done reading {count} bools");
                                break;
                            }
                        case ColumnTypeKind.Float:
                            {
                                Console.WriteLine($"Reading floats from column {columnId} ({columnName})");
                                var reader = new FloatReader(stripeStreamCollection, (uint)columnId);
                                var count = reader.Read().Count();
                                Console.WriteLine($"Done reading {count} floats");
                                break;
                            }
                        case ColumnTypeKind.Double:
                            {
                                Console.WriteLine($"Reading doubles from column {columnId} ({columnName})");
                                var reader = new DoubleReader(stripeStreamCollection, (uint)columnId);
                                var count = reader.Read().Count();
                                Console.WriteLine($"Done reading {count} doubles");
                                break;
                            }
                        case ColumnTypeKind.Binary:
                            {
                                Console.WriteLine($"Reading binary from column {columnId} ({columnName})");
                                var reader = new ColumnTypes.BinaryReader(stripeStreamCollection, (uint)columnId);
                                var count = reader.Read().Count();
                                Console.WriteLine($"Done reading {count} binaries");
                                break;
                            }
                        case ColumnTypeKind.Decimal:
                            {
                                Console.WriteLine($"Reading decimals from column {columnId} ({columnName})");
                                var reader = new DecimalReader(stripeStreamCollection, (uint)columnId);
                                var count = reader.Read().Count();
                                Console.WriteLine($"Done reading {count} decimals");
                                break;
                            }
                        case ColumnTypeKind.Timestamp:
                            {
                                Console.WriteLine($"Reading timestamps from column {columnId} ({columnName})");
                                var reader = new TimestampReader(stripeStreamCollection, (uint)columnId);
                                var count = reader.Read().Count();
                                Console.WriteLine($"Done reading {count} timestamps");
                                break;
                            }
                        case ColumnTypeKind.Date:
                            {
                                Console.WriteLine($"Reading dates from column {columnId} ({columnName})");
                                var reader = new DateReader(stripeStreamCollection, (uint)columnId);
                                var count = reader.Read().Count();
                                Console.WriteLine($"Done reading {count} dates");
                                break;
                            }
                        case ColumnTypeKind.String:
                            {
                                Console.WriteLine($"Reading strings from column {columnId} ({columnName})");
                                var reader = new ColumnTypes.StringReader(stripeStreamCollection, (uint)columnId);
                                var count = reader.Read().Count();
                                Console.WriteLine($"Done reading {count} strings");
                                break;
                            }
                        default:
                            throw new NotImplementedException();
                    }
                }

                Console.WriteLine("Done reading stripe");
            }
        }
    }
}
