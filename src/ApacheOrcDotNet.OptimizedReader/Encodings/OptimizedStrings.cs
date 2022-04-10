//using System;
//using System.Buffers;
//using System.Runtime.CompilerServices;

//namespace ApacheOrcDotNet.OptimizedReader.Encodings
//{
//    public static class OptimizedStrings
//    {
//        [SkipLocalsInit]
//        public static void ReadDirectV2(Span<string> outputValues)
//        {
//            var numValuesRead = 0;
//            var maxValuesToRead = outputValues.Length;
//            var presentBuffer = ArrayPool<bool>.Shared.Rent(maxValuesToRead);
//            var lengthsBuffer = ArrayPool<long>.Shared.Rent(maxValuesToRead);

//            try
//            {
//                ReadBooleanStream(StreamKind.Present, presentBuffer.AsSpan().Slice(0, maxValuesToRead), out var numPresentValuesRead);
//                ReadNumericStream(StreamKind.Length, isSigned: false, lengthsBuffer.AsSpan().Slice(0, maxValuesToRead), out var numLengthValuesRead);

//                ReadBinaryStream(
//                    StreamKind.Data,
//                    presentBuffer.AsSpan().Slice(0, numPresentValuesRead),
//                    lengthsBuffer.AsSpan().Slice(0, numLengthValuesRead),
//                    outputValues,
//                    ref numValuesRead
//                );
//            }
//            finally
//            {
//                ArrayPool<bool>.Shared.Return(presentBuffer, clearArray: false);
//                ArrayPool<long>.Shared.Return(lengthsBuffer, clearArray: false);
//            }
//        }
//    }
//}


//using ApacheOrcDotNet.Protocol;
//using System;
//using System.Buffers;
//using System.IO;
//using System.Runtime.CompilerServices;

//namespace ApacheOrcDotNet.OptimizedReader.ColumTypes
//{
//    public class OptimizedStringReader : OptimizedColumnReader
//    {
//        public OptimizedStringReader(SpanFileTail fileTail, IByteRangeProvider byteRangeProvider, ReadContext readContext) : base(fileTail, byteRangeProvider, readContext)
//        {
//        }

//        [SkipLocalsInit]
//        public int Read(Span<string> outputValues) => GetColumnEncodingKind(StreamKind.Data) switch
//        {
//            ColumnEncodingKind.DirectV2 => ReadDirectV2(outputValues),
//            ColumnEncodingKind.DictionaryV2 => ReadDictionaryV2(outputValues),
//            _ => throw new NotImplementedException($"Unsupported column encoding: {GetColumnEncodingKind(StreamKind.Data)}")
//        };

//        [SkipLocalsInit]
//        private int ReadDirectV2(Span<string> outputValues)
//        {
//            var numValuesRead = 0;
//            var maxValuesToRead = outputValues.Length;
//            var presentBuffer = ArrayPool<bool>.Shared.Rent(maxValuesToRead);
//            var lengthsBuffer = ArrayPool<long>.Shared.Rent(maxValuesToRead);

//            try
//            {
//                ReadBooleanStream(StreamKind.Present, presentBuffer.AsSpan().Slice(0, maxValuesToRead), out var numPresentValuesRead);
//                ReadNumericStream(StreamKind.Length, isSigned: false, lengthsBuffer.AsSpan().Slice(0, maxValuesToRead), out var numLengthValuesRead);

//                ReadBinaryStream(StreamKind.Data, presentBuffer, lengthsBuffer, outputValues, ref numValuesRead);

//                //if (numValuesRead != numLengthValuesRead)
//                //    throw new InvalidDataException("DATA and LENGTH streams must be available");

//                //int stringOffset = 0;
//                //if (numPresentValuesRead <= 0)
//                //{
//                //    for (int idx = 0; idx < numLengthValuesRead; idx++)
//                //    {
//                //        var length = (int)lengthsBuffer[idx];
//                //        outputValues[numValuesRead++] = Encoding.UTF8.GetString(dataBufferSpan.Slice(stringOffset, length));
//                //        stringOffset += length;
//                //    }
//                //}
//                //else
//                //{
//                //    for (int idx = 0; idx < numPresentValuesRead; idx++)
//                //    {
//                //        var isPresent = presentBuffer[idx];
//                //        if (isPresent)
//                //        {
//                //            if (idx + stringOffset >= numLengthValuesRead)
//                //                throw new InvalidDataException("The PRESENT data stream's length didn't match the LENGTH stream's length");
//                //            var length = (int)lengthsBuffer[idx];
//                //            outputValues[numValuesRead++] = Encoding.UTF8.GetString(dataBufferSpan.Slice(stringOffset, length));
//                //            stringOffset += length;
//                //        }
//                //        else
//                //            outputValues[numValuesRead++] = null;
//                //    }
//                //}

//                return numValuesRead;
//            }
//            finally
//            {
//                ArrayPool<bool>.Shared.Return(presentBuffer, clearArray: false);
//                ArrayPool<long>.Shared.Return(lengthsBuffer, clearArray: false);
//            }
//        }

//        [SkipLocalsInit]
//        private int ReadDictionaryV2(Span<string> outputValues)
//        {
//            var numValuesRead = 0;
//            var maxValuesToRead = outputValues.Length;
//            var presentBuffer = ArrayPool<bool>.Shared.Rent(maxValuesToRead);
//            var lengthBuffer = ArrayPool<long>.Shared.Rent(maxValuesToRead);
//            var dataBuffer = ArrayPool<long>.Shared.Rent(maxValuesToRead);

//            try
//            {
//                if (!IsDataStreamAvailable || !IsDictionaryDataStreamAvailable || !IsLengthStreamAvailable)
//                    throw new InvalidDataException("DATA, DICTIONARY_DATA, and LENGTH streams must be available");

//                ReadBooleanStream(StreamKind.Present, presentBuffer.AsSpan().Slice(0, maxValuesToRead), out var numPresentValuesRead);
//                ReadNumericStream(StreamKind.Data, isSigned: false, dataBuffer.AsSpan().Slice(0, maxValuesToRead), out var numDataValuesRead);
//                ReadNumericStream(StreamKind.Length, isSigned: false, lengthBuffer.AsSpan().Slice(0, maxValuesToRead), out var numLengthValuesRead);

//                ReadBinaryStream(
//                    StreamKind.DictionaryData,
//                    presentBuffer.AsSpan().Slice(0, numPresentValuesRead),
//                    lengthBuffer.AsSpan().Slice(0, numLengthValuesRead),
//                    outputValues,
//                    ref numValuesRead
//                 );

//                //int stringOffset = 0;
//                //List<string> stringsList = new();
//                //for (int idx = 0; idx < numLengthValuesRead; idx++)
//                //{
//                //    var length = (int)lengthBuffer[idx];
//                //    var value = Encoding.UTF8.GetString(dictionaryBufferSpan.Slice(stringOffset, length));
//                //    stringOffset += length;
//                //    stringsList.Add(value);
//                //}

//                //if (numPresentValuesRead <= 0)
//                //{
//                //    for (int idx = 0; idx < numDataValuesRead; idx++)
//                //        outputValues[numValuesRead++] = stringsList[(int)dataBuffer[idx]];
//                //}
//                //else
//                //{
//                //    for (int idx = 0; idx < numPresentValuesRead; idx++)
//                //    {
//                //        var isPresent = presentBuffer[idx];
//                //        if (isPresent)
//                //        {
//                //            if (idx >= numDataValuesRead)
//                //                throw new InvalidDataException("The PRESENT data stream's length didn't match the DATA stream's length");
//                //            outputValues[numValuesRead++] = stringsList[(int)dataBuffer[idx]];
//                //        }
//                //        else
//                //            outputValues[numValuesRead++] = null;
//                //    }
//                //}

//                return numValuesRead;
//            }
//            finally
//            {
//                ArrayPool<bool>.Shared.Return(presentBuffer, clearArray: false);
//                ArrayPool<long>.Shared.Return(dataBuffer, clearArray: false);
//                ArrayPool<long>.Shared.Return(lengthBuffer, clearArray: false);
//            }
//        }
//    }
//}
