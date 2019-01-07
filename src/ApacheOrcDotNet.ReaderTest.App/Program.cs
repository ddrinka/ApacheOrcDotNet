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
                            {
                                Console.WriteLine($"Reading longs from column {columnId} ({columnName})");
                                var reader = new LongReader(stripeStreamCollection, (uint)columnId);
                                reader.Read().ToList();
                                Console.WriteLine("Done reading longs");
                                break;
                            }
                        case ColumnTypeKind.String:
                            {
                                Console.WriteLine($"Reading strings from column {columnId} ({columnName})");
                                var reader = new ColumnTypes.StringReader(stripeStreamCollection, (uint)columnId);
                                reader.Read().ToList();
                                Console.WriteLine("Done reading strings");
                                break;
                            }
                        case ColumnTypeKind.Decimal:
                            {
                                Console.WriteLine($"Reading decimals from column {columnId} ({columnName})");
                                var reader = new DecimalReader(stripeStreamCollection, (uint)columnId);
                                reader.Read().ToList();
                                Console.WriteLine("Done reading decimals");
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
