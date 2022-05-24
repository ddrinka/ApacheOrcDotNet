using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace ApacheOrcDotNet.OptimizedReader.ColumTypes
{
    public class OptimizedStringReader : BaseColumnReader<string>
    {
        public OptimizedStringReader(ReaderContext readerContext) : base(readerContext)
        {
        }

        public override void FillBuffer()
        {
            switch (_readerContext.ColumnEncodingKind)
            {
                case ColumnEncodingKind.DirectV2:
                    ReadDirectV2();
                    break;
                case ColumnEncodingKind.DictionaryV2:
                    ReadDictionaryV2();
                    break;
                default:
                    throw new InvalidOperationException();
            }
        }

        private void ReadDirectV2()
        {
            var presentStream = GetStripeStream(StreamKind.Present, isRequired: false);
            var lengthStream = GetStripeStream(StreamKind.Length);
            var dataStream = GetStripeStream(StreamKind.Data);

            var presentBuffer = ArrayPool<bool>.Shared.Rent(_numMaxValuesToRead);
            var lengthsBuffer = ArrayPool<long>.Shared.Rent(_numMaxValuesToRead);

            try
            {
                // Present
                var presentPositions = GetPresentStreamPositions(presentStream);
                var numPresentValuesRead = ReadBooleanStream(presentStream, presentPositions, presentBuffer.AsSpan().Slice(0, _numMaxValuesToRead));

                // Length
                var lengthPositions = GetTargetedStreamPositions(presentStream, lengthStream);
                var numLengthValuesRead = ReadNumericStream(lengthStream, lengthPositions, isSigned: false, lengthsBuffer.AsSpan().Slice(0, _numMaxValuesToRead));

                // Data
                var dataStreamPostions = GetTargetedStreamPositions(presentStream, dataStream);
                var dataBuffer = _readerContext.ByteRangeProvider.DecompressByteRange(
                    offset: dataStream.FileOffset + dataStreamPostions.RowGroupOffset,
                    compressedLength: dataStream.Length - dataStreamPostions.RowGroupOffset,
                    compressionKind: _readerContext.CompressionKind,
                    compressionBlockSize: _readerContext.CompressionBlockSize
                );

                using (dataBuffer)
                {
                    var rowEntryLength = dataBuffer.Sequence.Length - dataStreamPostions.RowEntryOffset;
                    var dataSequence = dataBuffer.Sequence.Slice(dataStreamPostions.RowEntryOffset, rowEntryLength);

                    var stringOffset = 0;
                    if (presentStream != null)
                    {
                        var lengthIndex = 0;
                        for (int idx = 0; idx < numPresentValuesRead; idx++)
                        {
                            if (presentBuffer[idx])
                            {
                                var length = (int)lengthsBuffer[lengthIndex++];
                                _outputValuesRaw[_numValuesRead++] = Encoding.UTF8.GetString(dataSequence.Slice(stringOffset, length));
                                stringOffset += length;
                            }
                            else
                                _outputValuesRaw[_numValuesRead++] = null;

                            if (_numValuesRead >= _numMaxValuesToRead)
                                break;
                        }
                    }
                    else
                    {
                        for (int idx = 0; idx < numLengthValuesRead; idx++)
                        {
                            var length = (int)lengthsBuffer[idx];
                            _outputValuesRaw[_numValuesRead++] = Encoding.UTF8.GetString(dataSequence.Slice(stringOffset, length));
                            stringOffset += length;

                            if (_numValuesRead >= _numMaxValuesToRead)
                                break;
                        }
                    }
                }
            }
            finally
            {
                ArrayPool<bool>.Shared.Return(presentBuffer, clearArray: false);
                ArrayPool<long>.Shared.Return(lengthsBuffer, clearArray: false);
            }
        }

        [SkipLocalsInit]
        private void ReadDictionaryV2()
        {
            var presentStream = GetStripeStream(StreamKind.Present, isRequired: false);
            var lengthStream = GetStripeStream(StreamKind.Length);
            var dataStream = GetStripeStream(StreamKind.Data);
            var dictionaryDataStream = GetStripeStream(StreamKind.DictionaryData);

            var dictionarySize = lengthStream.DictionarySize;

            var presentBuffer = ArrayPool<bool>.Shared.Rent(_numMaxValuesToRead);
            var lengthBuffer = ArrayPool<long>.Shared.Rent(dictionarySize);
            var dataBuffer = ArrayPool<long>.Shared.Rent(_numMaxValuesToRead);

            // Present
            var presentPositions = GetPresentStreamPositions(presentStream);
            var numPresentValuesRead = ReadBooleanStream(presentStream, in presentPositions, presentBuffer.AsSpan().Slice(0, _numMaxValuesToRead));

            // Length
            var lengthPositions = GetTargetedStreamPositions(presentStream, lengthStream);
            var numLengthValuesRead = ReadNumericStream(lengthStream, in lengthPositions, isSigned: false, lengthBuffer.AsSpan().Slice(0, dictionarySize));

            // Data
            var dataPostions = GetTargetedStreamPositions(presentStream, dataStream);
            var numDataValuesRead = ReadNumericStream(dataStream, in dataPostions, isSigned: false, dataBuffer.AsSpan().Slice(0, _numMaxValuesToRead));

            // Dictionary Data
            var dictionaryDataPositions = GetTargetedStreamPositions(presentStream, dictionaryDataStream);
            var decompressedBuffer = _readerContext.ByteRangeProvider.DecompressByteRange(
                offset: dictionaryDataStream.FileOffset + dictionaryDataPositions.RowGroupOffset,
                compressedLength: dictionaryDataStream.Length - dictionaryDataPositions.RowGroupOffset,
                compressionKind: _readerContext.CompressionKind,
                compressionBlockSize: _readerContext.CompressionBlockSize
            );

            using (decompressedBuffer)
            {
                int stringOffset = 0;
                List<string> stringsList = new(numLengthValuesRead);
                for (int idx = 0; idx < numLengthValuesRead; idx++)
                {
                    var length = (int)lengthBuffer[idx];
                    var value = Encoding.UTF8.GetString(decompressedBuffer.Sequence.Slice(stringOffset, length));
                    stringOffset += length;
                    stringsList.Add(value);
                }

                if (presentStream != null)
                {
                    var dataIndex = 0;
                    for (int idx = 0; idx < numPresentValuesRead; idx++)
                    {
                        if (presentBuffer[idx])
                            _outputValuesRaw[_numValuesRead++] = stringsList[(int)dataBuffer[dataIndex++]];
                        else
                            _outputValuesRaw[_numValuesRead++] = null;

                        if (_numValuesRead >= _numMaxValuesToRead)
                            break;
                    }
                }
                else
                {
                    for (int idx = 0; idx < numDataValuesRead; idx++)
                    {
                        _outputValuesRaw[_numValuesRead++] = stringsList[(int)dataBuffer[idx]];

                        if (_numValuesRead >= _numMaxValuesToRead)
                            break;
                    }
                }
            }

            ArrayPool<bool>.Shared.Return(presentBuffer, clearArray: false);
            ArrayPool<long>.Shared.Return(dataBuffer, clearArray: false);
            ArrayPool<long>.Shared.Return(lengthBuffer, clearArray: false);
        }
    }
}
