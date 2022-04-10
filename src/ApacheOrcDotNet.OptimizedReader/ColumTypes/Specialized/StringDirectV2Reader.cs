﻿using ApacheOrcDotNet.Protocol;
using System;
using System.Buffers;
using System.Text;

namespace ApacheOrcDotNet.OptimizedReader.ColumTypes.Specialized
{
    public class StringDirectV2Reader : BaseColumnReader<string>
    {
        public StringDirectV2Reader(ReaderContext readerContext) : base(readerContext)
        {
        }

        public override void FillBuffer()
        {
            var presentStreamRequired = _readerContext.RowIndexEntry.Statistics.HasNull;

            var presentStream = GetStripeStream(StreamKind.Present, presentStreamRequired);
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

                var rowEntryLength = dataBuffer.Sequence.Length - dataStreamPostions.RowEntryOffset;
                var dataSequence = dataBuffer.Sequence.Slice(dataStreamPostions.RowEntryOffset, rowEntryLength);

                var stringOffset = 0;
                if (presentStreamRequired)
                {
                    var lengthIndex = 0;
                    for (int idx = 0; idx < numPresentValuesRead; idx++)
                    {
                        if (presentBuffer[idx])
                        {
                            var length = (int)lengthsBuffer[lengthIndex++];
                            _outputValuesRaw[_numValuesRead++] = Encoding.UTF8.GetString(dataBuffer.Sequence.Slice(stringOffset, length));
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
            finally
            {
                ArrayPool<bool>.Shared.Return(presentBuffer, clearArray: false);
                ArrayPool<long>.Shared.Return(lengthsBuffer, clearArray: false);
            }
        }
    }
}
