using ApacheOrcDotNet.OptimizedReader.Infrastructure;
using ApacheOrcDotNet.Protocol;
using System;
using System.Buffers;

namespace ApacheOrcDotNet.OptimizedReader.ColumTypes
{
    public class OptimizedDoubleReader : BaseColumnReader<double>
    {
        public OptimizedDoubleReader(ReaderContext readerContext) : base(readerContext)
        {
        }

        public override void FillBuffer()
        {
            var presentStream = GetStripeStream(StreamKind.Present, isRequired: false);
            var dataStream = GetStripeStream(StreamKind.Data);

            var presentBuffer = ArrayPool<bool>.Shared.Rent(_numMaxValuesToRead);

            try
            {
                // Present
                var presentPositions = GetPresentStreamPositions(presentStream);
                var numPresentValuesRead = ReadBooleanStream(presentStream, presentPositions, presentBuffer.AsSpan().Slice(0, _numMaxValuesToRead));

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
                    var dataReader = new SequenceReader<byte>(dataSequence);

                    Span<byte> valueBuffer = stackalloc byte[8];
                    if (presentStream != null)
                    {
                        for (int idx = 0; idx < numPresentValuesRead; idx++)
                        {
                            if (presentBuffer[idx])
                            {
                                dataReader.TryCopyTo(valueBuffer);
                                _outputValuesRaw[_numValuesRead++] = BitConverter.ToDouble(valueBuffer);
                                dataReader.Advance(valueBuffer.Length);
                            }
                            else
                                _outputValuesRaw[_numValuesRead++] = double.NaN;
                        }
                    }
                    else
                    { 
                        while (dataReader.TryCopyTo(valueBuffer))
                        {
                            dataReader.Advance(valueBuffer.Length);

                            _outputValuesRaw[_numValuesRead++] = BitConverter.ToDouble(valueBuffer);

                            if (_numValuesRead >= _numMaxValuesToRead)
                                break;
                        }
                    }
                }
            }
            finally
            {
                ArrayPool<bool>.Shared.Return(presentBuffer);
            }
        }
    }
}