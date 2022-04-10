using ApacheOrcDotNet.Protocol;
using System;
using System.Buffers;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace ApacheOrcDotNet.OptimizedReader.ColumTypes
{
    public class OptimizedDecimalReader : OptimizedColumnReader
    {
        private readonly SpanFileTail _fileTail;
        private readonly IByteRangeProvider _byteRangeProvider;
        private readonly ReaderContextOld _readContext;

        public OptimizedDecimalReader(SpanFileTail fileTail, IByteRangeProvider byteRangeProvider, ReaderContextOld readContext) : base(fileTail, byteRangeProvider, readContext)
        {
            _fileTail = fileTail;
            _byteRangeProvider = byteRangeProvider;
            _readContext = readContext;
        }

        [SkipLocalsInit]
        public int Read(Span<decimal> outputValues)
        {
            var maxValuesToRead = (int)_fileTail.Footer.RowIndexStride;

            var dataStream = GetStripeStream(StreamKind.Data);
            var secondaryStream = GetStripeStream(StreamKind.Secondary);

            var dataBuffer = ArrayPool<BigInteger>.Shared.Rent(maxValuesToRead);
            var secondaryBuffer = ArrayPool<long>.Shared.Rent(maxValuesToRead);

            try
            {
                // Data
                var dataPostions = _readContext.GetTargetedStreamPositions(presentStream: null, dataStream);
                var numDataValuesRead = ReadVarIntStream(dataStream, dataPostions, dataBuffer.AsSpan().Slice(0, maxValuesToRead));

                // Secondary
                var secondaryPostions = _readContext.GetTargetedStreamPositions(presentStream: null, secondaryStream);
                var numSecondaryValuesRead = ReadNumericStream(secondaryStream, secondaryPostions, isSigned: true, secondaryBuffer.AsSpan().Slice(0, maxValuesToRead));

                var numValuesRead = 0;
                for (int idx = 0; idx < numSecondaryValuesRead; idx++)
                    outputValues[numValuesRead++] = BigIntegerToDecimal(dataBuffer[idx], secondaryBuffer[idx]);

                return numValuesRead;
            }
            finally
            {
                ArrayPool<BigInteger>.Shared.Return(dataBuffer);
                ArrayPool<long>.Shared.Return(secondaryBuffer);
            }
        }

        [SkipLocalsInit]
        public int Read(Span<decimal?> outputValues)
        {
            var maxValuesToRead = (int)_fileTail.Footer.RowIndexStride;
            var presentStreamRequired = _readContext.RowIndexEntry.Statistics.HasNull;

            var presentStream = GetStripeStream(StreamKind.Present, presentStreamRequired);
            var dataStream = GetStripeStream(StreamKind.Data);
            var secondaryStream = GetStripeStream(StreamKind.Secondary);

            var presentBuffer = ArrayPool<bool>.Shared.Rent(maxValuesToRead);
            var dataBuffer = ArrayPool<BigInteger>.Shared.Rent(maxValuesToRead);
            var secondaryBuffer = ArrayPool<long>.Shared.Rent(maxValuesToRead);

            try
            {
                // Present
                var presentPositions = _readContext.GetPresentStreamPositions(presentStream);
                var numPresentValuesRead = ReadBooleanStream(presentStream, presentPositions, presentBuffer.AsSpan().Slice(0, maxValuesToRead));

                // Data
                var dataPostions = _readContext.GetTargetedStreamPositions(presentStream, dataStream);
                var numDataValuesRead = ReadVarIntStream(dataStream, dataPostions, dataBuffer.AsSpan().Slice(0, maxValuesToRead));

                // Secondary
                var secondaryPostions = _readContext.GetTargetedStreamPositions(presentStream, secondaryStream);
                var numSecondaryValuesRead = ReadNumericStream(secondaryStream, secondaryPostions, isSigned: true, secondaryBuffer.AsSpan().Slice(0, maxValuesToRead));

                var numValuesRead = 0;
                var secondaryIndex = 0;
                if (presentStreamRequired)
                {
                    for (int idx = 0; idx < numPresentValuesRead; idx++)
                    {
                        if (presentBuffer[idx])
                        {
                            outputValues[numValuesRead++] = BigIntegerToDecimal(dataBuffer[secondaryIndex], secondaryBuffer[secondaryIndex]);
                            secondaryIndex++;
                        }
                        else
                            outputValues[numValuesRead++] = null;
                    }
                }
                else
                {
                    if (numDataValuesRead != numSecondaryValuesRead)
                        throw new InvalidOperationException("Number of values read from DATA and SECODARY streams do not match.");

                    for (int idx = 0; idx < numSecondaryValuesRead; idx++)
                        outputValues[numValuesRead++] = BigIntegerToDecimal(dataBuffer[idx], secondaryBuffer[idx]);
                }

                return numValuesRead;
            }
            finally
            {
                ArrayPool<bool>.Shared.Return(presentBuffer);
                ArrayPool<BigInteger>.Shared.Return(dataBuffer);
                ArrayPool<long>.Shared.Return(secondaryBuffer);
            }
        }

        [SkipLocalsInit]
        public int ReadAsDouble(Span<double> outputValues)
        {
            var maxValuesToRead = (int)_fileTail.Footer.RowIndexStride;
            var presentStreamRequired = _readContext.RowIndexEntry.Statistics.HasNull;

            var presentStream = GetStripeStream(StreamKind.Present, presentStreamRequired);
            var dataStream = GetStripeStream(StreamKind.Data);
            var secondaryStream = GetStripeStream(StreamKind.Secondary);

            var presentBuffer = ArrayPool<bool>.Shared.Rent(maxValuesToRead);
            var dataBuffer = ArrayPool<BigInteger>.Shared.Rent(maxValuesToRead);
            var secondaryBuffer = ArrayPool<long>.Shared.Rent(maxValuesToRead);

            try
            {
                // Present
                var presentPositions = _readContext.GetPresentStreamPositions(presentStream);
                var numPresentValuesRead = ReadBooleanStream(presentStream, presentPositions, presentBuffer.AsSpan().Slice(0, maxValuesToRead));

                // Data
                var dataPostions = _readContext.GetTargetedStreamPositions(presentStream, dataStream);
                var numDataValuesRead = ReadVarIntStream(dataStream, dataPostions, dataBuffer.AsSpan().Slice(0, maxValuesToRead));

                // Secondary
                var secondaryPostions = _readContext.GetTargetedStreamPositions(presentStream, secondaryStream);
                var numSecondaryValuesRead = ReadNumericStream(secondaryStream, secondaryPostions, isSigned: true, secondaryBuffer.AsSpan().Slice(0, maxValuesToRead));

                var numValuesRead = 0;
                if (presentStreamRequired)
                {
                    var secondaryIndex = 0;
                    for (int idx = 0; idx < numPresentValuesRead; idx++)
                    {
                        if (presentBuffer[idx])
                            outputValues[numValuesRead++] = BigIntegerToDouble(dataBuffer[idx], secondaryBuffer[secondaryIndex++]);
                        else
                            outputValues[numValuesRead++] = double.NaN;
                    }
                }
                else
                {
                    for (int idx = 0; idx < numDataValuesRead; idx++)
                        outputValues[numValuesRead++] = BigIntegerToDouble(dataBuffer[idx], secondaryBuffer[idx]);
                }

                return numValuesRead;
            }
            finally
            {
                ArrayPool<bool>.Shared.Return(presentBuffer);
                ArrayPool<BigInteger>.Shared.Return(dataBuffer);
                ArrayPool<long>.Shared.Return(secondaryBuffer);
            }
        }

        private double BigIntegerToDouble(BigInteger numerator, long scale)
            => (double)BigIntegerToDecimal(numerator, scale);

        private decimal BigIntegerToDecimal(BigInteger numerator, long scale)
        {
            if (scale < 0 || scale > 255)
                throw new OverflowException("Scale must be positive number");

            var decNumerator = (decimal)numerator; //This will throw for an overflow or underflow
            var scaler = new decimal(1, 0, 0, false, (byte)scale);

            return decNumerator * scaler;
        }
    }
}
