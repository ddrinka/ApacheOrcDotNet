using ApacheOrcDotNet.Protocol;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;

namespace ApacheOrcDotNet.OptimizedReader.ColumTypes.Specialized
{
    public class StringDictionaryV2Reader : BaseColumnReader<string>
    {
        public StringDictionaryV2Reader(ReaderContext readerContext) : base(readerContext)
        {
        }

        [SkipLocalsInit]
        public override void FillBuffer()
        {
            var presentStreamRequired = _readerContext.RowIndexEntry.Statistics.HasNull;

            var presentStream = GetStripeStream(StreamKind.Present, isRequired: presentStreamRequired);
            var lengthStream = GetStripeStream(StreamKind.Length);
            var dataStream = GetStripeStream(StreamKind.Data);
            var dictionaryDataStream = GetStripeStream(StreamKind.DictionaryData);

            var dictionarySize = lengthStream.DictionarySize;

            var presentBuffer = ArrayPool<bool>.Shared.Rent(_numMaxValuesToRead);
            var lengthBuffer = ArrayPool<long>.Shared.Rent(dictionarySize);
            var dataBuffer = ArrayPool<long>.Shared.Rent(_numMaxValuesToRead);

            try
            {
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

                int stringOffset = 0;
                List<string> stringsList = new(numLengthValuesRead);
                for (int idx = 0; idx < numLengthValuesRead; idx++)
                {
                    var length = (int)lengthBuffer[idx];
                    var value = Encoding.UTF8.GetString(decompressedBuffer.Sequence.Slice(stringOffset, length));
                    stringOffset += length;
                    stringsList.Add(value);
                }

                if (presentStreamRequired)
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
            finally
            {
                ArrayPool<bool>.Shared.Return(presentBuffer, clearArray: false);
                ArrayPool<long>.Shared.Return(dataBuffer, clearArray: false);
                ArrayPool<long>.Shared.Return(lengthBuffer, clearArray: false);
            }
        }
    }
}
