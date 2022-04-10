using ApacheOrcDotNet.Protocol;
using System;
using System.Buffers;
using System.Numerics;

namespace ApacheOrcDotNet.OptimizedReader.ColumTypes.Specialized
{
    public class DecimalAsDoubleDirectV2Reader : BaseColumnReader<double>
    {
        public DecimalAsDoubleDirectV2Reader(ReaderContext readerContext) : base(readerContext)
        {
        }

        public override void FillBuffer()
        {
            var presentStreamRequired = _readerContext.RowIndexEntry.Statistics.HasNull;

            var presentStream = GetStripeStream(StreamKind.Present, presentStreamRequired);
            var dataStream = GetStripeStream(StreamKind.Data);
            var secondaryStream = GetStripeStream(StreamKind.Secondary);

            var presentBuffer = ArrayPool<bool>.Shared.Rent(_numMaxValuesToRead);
            var dataBuffer = ArrayPool<BigInteger>.Shared.Rent(_numMaxValuesToRead);
            var secondaryBuffer = ArrayPool<long>.Shared.Rent(_numMaxValuesToRead);

            try
            {
                // Present
                var presentPositions = GetPresentStreamPositions(presentStream);
                var numPresentValuesRead = ReadBooleanStream(presentStream, presentPositions, presentBuffer.AsSpan().Slice(0, _numMaxValuesToRead));

                // Data
                var dataPostions = GetTargetedStreamPositions(presentStream, dataStream);
                var numDataValuesRead = ReadVarIntStream(dataStream, dataPostions, dataBuffer.AsSpan().Slice(0, _numMaxValuesToRead));

                // Secondary
                var secondaryPostions = GetTargetedStreamPositions(presentStream, secondaryStream);
                var numSecondaryValuesRead = ReadNumericStream(secondaryStream, secondaryPostions, isSigned: true, secondaryBuffer.AsSpan().Slice(0, _numMaxValuesToRead));

                var secondaryIndex = 0;
                if (presentStreamRequired)
                {
                    for (int idx = 0; idx < numPresentValuesRead; idx++)
                    {
                        if (presentBuffer[idx])
                        {
                            _outputValuesRaw[_numValuesRead++] = BigIntegerToDouble(dataBuffer[secondaryIndex], secondaryBuffer[secondaryIndex]);
                            secondaryIndex++;
                        }
                        else
                            _outputValuesRaw[_numValuesRead++] = double.NaN;
                    }
                }
                else
                {
                    if (numDataValuesRead != numSecondaryValuesRead)
                        throw new InvalidOperationException("Number of values read from DATA and SECODARY streams do not match.");

                    for (int idx = 0; idx < numSecondaryValuesRead; idx++)
                        _outputValuesRaw[_numValuesRead++] = BigIntegerToDouble(dataBuffer[idx], secondaryBuffer[idx]);
                }
            }
            finally
            {
                ArrayPool<bool>.Shared.Return(presentBuffer);
                ArrayPool<BigInteger>.Shared.Return(dataBuffer);
                ArrayPool<long>.Shared.Return(secondaryBuffer);
            }
        }
    }
}
