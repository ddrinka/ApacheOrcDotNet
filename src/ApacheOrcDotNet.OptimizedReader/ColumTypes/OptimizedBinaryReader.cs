using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System;
using System.Buffers;

namespace ApacheOrcDotNet.OptimizedReader.ColumTypes
{
    public class OptimizedBinaryReader : BaseColumnReader<byte[]>
    {
        public OptimizedBinaryReader(ReaderContext readerContext) : base(readerContext)
        {
        }

        public override void FillBuffer()
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
                                _outputValuesRaw[_numValuesRead++] = dataSequence.Slice(stringOffset, length).ToArray();
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
                            _outputValuesRaw[_numValuesRead++] = dataSequence.Slice(stringOffset, length).ToArray();
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
    }
}