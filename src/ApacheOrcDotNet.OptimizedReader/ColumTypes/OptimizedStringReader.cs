using ApacheOrcDotNet.Protocol;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace ApacheOrcDotNet.OptimizedReader.ColumTypes
{
    public class OptimizedStringReader : OptimizedColumnReader
    {
        private readonly SpanFileTail _fileTail;
        private readonly IByteRangeProvider _byteRangeProvider;
        private readonly ReaderContextOld _readContext;

        public OptimizedStringReader(SpanFileTail fileTail, IByteRangeProvider byteRangeProvider, ReaderContextOld readContext) : base(fileTail, byteRangeProvider, readContext)
        {
            _fileTail = fileTail;
            _byteRangeProvider = byteRangeProvider;
            _readContext = readContext;
        }

        [SkipLocalsInit]
        public int Read(Span<string> outputValues) => GetColumnEncodingKind(StreamKind.Data) switch
        {
            ColumnEncodingKind.DirectV2 => ReadDirectV2(outputValues),
            ColumnEncodingKind.DictionaryV2 => ReadDictionaryV2(outputValues),
            _ => throw new NotImplementedException($"Unsupported column encoding: {GetColumnEncodingKind(StreamKind.Data)}")
        };

        [SkipLocalsInit]
        private int ReadDirectV2(Span<string> outputValues)
        {
            var maxValuesToRead = (int)_fileTail.Footer.RowIndexStride;
            var presentStreamRequired = _readContext.RowIndexEntry.Statistics.HasNull;

            var presentStream = GetStripeStream(StreamKind.Present, presentStreamRequired);
            var lengthStream = GetStripeStream(StreamKind.Length);
            var dataStream = GetStripeStream(StreamKind.Data);

            var presentBuffer = ArrayPool<bool>.Shared.Rent(maxValuesToRead);
            var lengthsBuffer = ArrayPool<long>.Shared.Rent(maxValuesToRead);

            try
            {
                // Present
                var presentPositions = _readContext.GetPresentStreamPositions(presentStream);
                var numPresentValuesRead = ReadBooleanStream(presentStream, presentPositions, presentBuffer.AsSpan().Slice(0, maxValuesToRead));

                // Length
                var lengthPositions = _readContext.GetTargetedStreamPositions(presentStream, lengthStream);
                var numLengthValuesRead = ReadNumericStream(lengthStream, lengthPositions, isSigned: false, lengthsBuffer.AsSpan().Slice(0, maxValuesToRead));

                // Data
                var dataStreamPostions = _readContext.GetTargetedStreamPositions(presentStream, dataStream);
                var dataBuffer = _byteRangeProvider.DecompressByteRange(
                    offset: dataStream.FileOffset + dataStreamPostions.RowGroupOffset,
                    compressedLength: dataStream.Length - dataStreamPostions.RowGroupOffset,
                    compressionKind: _fileTail.PostScript.Compression,
                    compressionBlockSize: (int)_fileTail.PostScript.CompressionBlockSize
                );

                var rowEntryLength = dataBuffer.Sequence.Length - dataStreamPostions.RowEntryOffset;
                var dataSequence = dataBuffer.Sequence.Slice(dataStreamPostions.RowEntryOffset, rowEntryLength);

                var stringOffset = 0;
                var numValuesRead = 0;
                if (presentStreamRequired)
                {
                    var lengthIndex = 0;
                    for (int idx = 0; idx < numPresentValuesRead; idx++)
                    {
                        if (presentBuffer[idx])
                        {
                            var length = (int)lengthsBuffer[lengthIndex++];
                            outputValues[numValuesRead++] = Encoding.UTF8.GetString(dataBuffer.Sequence.Slice(stringOffset, length));
                            stringOffset += length;
                        }
                        else
                            outputValues[numValuesRead++] = null;

                        if (numValuesRead >= outputValues.Length)
                            break;
                    }
                }
                else
                {
                    for (int idx = 0; idx < numLengthValuesRead; idx++)
                    {
                        var length = (int)lengthsBuffer[idx];
                        outputValues[numValuesRead++] = Encoding.UTF8.GetString(dataSequence.Slice(stringOffset, length));
                        stringOffset += length;

                        if (numValuesRead >= outputValues.Length)
                            break;
                    }
                }

                return numValuesRead;
            }
            finally
            {
                ArrayPool<bool>.Shared.Return(presentBuffer, clearArray: false);
                ArrayPool<long>.Shared.Return(lengthsBuffer, clearArray: false);
            }
        }

        [SkipLocalsInit]
        private int ReadDictionaryV2(Span<string> outputValues)
        {
            var maxValuesToRead = (int)_fileTail.Footer.RowIndexStride;
            var presentStreamRequired = _readContext.RowIndexEntry.Statistics.HasNull;

            var presentStream = GetStripeStream(StreamKind.Present, isRequired: presentStreamRequired);
            var lengthStream = GetStripeStream(StreamKind.Length);
            var dataStream = GetStripeStream(StreamKind.Data);
            var dictionaryDataStream = GetStripeStream(StreamKind.DictionaryData);

            var dictionarySize = lengthStream.DictionarySize;

            var presentBuffer = ArrayPool<bool>.Shared.Rent(maxValuesToRead);
            var lengthBuffer = ArrayPool<long>.Shared.Rent(dictionarySize);
            var dataBuffer = ArrayPool<long>.Shared.Rent(maxValuesToRead);

            try
            {
                // Present
                var presentPositions = _readContext.GetPresentStreamPositions(presentStream);
                var numPresentValuesRead = ReadBooleanStream(presentStream, in presentPositions, presentBuffer.AsSpan().Slice(0, maxValuesToRead));

                // Length
                var lengthPositions = _readContext.GetTargetedStreamPositions(presentStream, lengthStream);
                var numLengthValuesRead = ReadNumericStream(lengthStream, in lengthPositions, isSigned: false, lengthBuffer.AsSpan().Slice(0, dictionarySize));

                // Data
                var dataPostions = _readContext.GetTargetedStreamPositions(presentStream, dataStream);
                var numDataValuesRead = ReadNumericStream(dataStream, in dataPostions, isSigned: false, dataBuffer.AsSpan().Slice(0, maxValuesToRead));

                // Dictionary Data
                var dictionaryDataPositions = _readContext.GetTargetedStreamPositions(presentStream, dictionaryDataStream);
                var decompressedBuffer = _byteRangeProvider.DecompressByteRange(
                    offset: dictionaryDataStream.FileOffset + dictionaryDataPositions.RowGroupOffset,
                    compressedLength: dictionaryDataStream.Length - dictionaryDataPositions.RowGroupOffset,
                    compressionKind: _fileTail.PostScript.Compression,
                    compressionBlockSize: (int)_fileTail.PostScript.CompressionBlockSize
                );

                int stringOffset = 0;
                List<string> stringsList = new(numLengthValuesRead);
                for (int idx = 0; idx < numLengthValuesRead; idx++)
                {
                    var length = (int)lengthBuffer[idx];
                    var value = Encoding.UTF8.GetString(decompressedBuffer.Sequence.Slice(stringOffset, length));
                    stringOffset += length;
                    stringsList.Add(value);
                }

                var numValuesRead = 0;
                if (presentStreamRequired)
                {
                    var dataIndex = 0;
                    for (int idx = 0; idx < numPresentValuesRead; idx++)
                    {
                        if (presentBuffer[idx])
                            outputValues[numValuesRead++] = stringsList[(int)dataBuffer[dataIndex++]];
                        else
                            outputValues[numValuesRead++] = null;

                        if (numValuesRead >= outputValues.Length)
                            break;
                    }
                }
                else
                {
                    for (int idx = 0; idx < numDataValuesRead; idx++)
                    {
                        outputValues[numValuesRead++] = stringsList[(int)dataBuffer[idx]];

                        if (numValuesRead >= outputValues.Length)
                            break;
                    }
                }

                return numValuesRead;
            }
            finally
            {
                ArrayPool<bool>.Shared.Return(presentBuffer, clearArray: false);
                ArrayPool<long>.Shared.Return(dataBuffer, clearArray: false);
                ArrayPool<long>.Shared.Return(lengthBuffer, clearArray: false);
            }
        }
    }
}
