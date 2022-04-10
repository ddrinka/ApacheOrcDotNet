using ApacheOrcDotNet.Protocol;
using System;
using System.Buffers;
using System.Runtime.CompilerServices;

namespace ApacheOrcDotNet.OptimizedReader.ColumTypes
{
    public class OptimizedLongReader : OptimizedColumnReader
    {
        private readonly SpanFileTail _fileTail;
        private readonly IByteRangeProvider _byteRangeProvider;
        private readonly ReaderContextOld _readContext;

        public OptimizedLongReader(SpanFileTail fileTail, IByteRangeProvider byteRangeProvider, ReaderContextOld readContext) : base(fileTail, byteRangeProvider, readContext)
        {
            _fileTail = fileTail;
            _byteRangeProvider = byteRangeProvider;
            _readContext = readContext;
        }

        [SkipLocalsInit]
        public int Read(Span<long> outputValues)
        {
            var maxValuesToRead = (int)_fileTail.Footer.RowIndexStride;
            var dataStream = GetStripeStream(StreamKind.Data);
            var dataBuffer = ArrayPool<long>.Shared.Rent(maxValuesToRead);

            try
            {
                // Data
                var dataStreamPostions = _readContext.GetTargetedStreamPositions(presentStream: null, dataStream);
                var numDataValuesRead = ReadNumericStream(dataStream, dataStreamPostions, isSigned: true, dataBuffer.AsSpan().Slice(0, maxValuesToRead));

                var numValuesRead = 0;
                for (int idx = 0; idx < numDataValuesRead; idx++)
                    outputValues[numValuesRead++] = dataBuffer[idx];

                return numValuesRead;
            }
            finally
            {
                ArrayPool<long>.Shared.Return(dataBuffer);
            }
        }

        [SkipLocalsInit]
        public int Read(Span<long?> outputValues)
        {
            var maxValuesToRead = (int)_fileTail.Footer.RowIndexStride;
            var presentStreamRequired = _readContext.RowIndexEntry.Statistics.HasNull;

            var presentStream = GetStripeStream(StreamKind.Present, presentStreamRequired);
            var dataStream = GetStripeStream(StreamKind.Data);

            var presentBuffer = ArrayPool<bool>.Shared.Rent(maxValuesToRead);
            var dataBuffer = ArrayPool<long>.Shared.Rent(maxValuesToRead);

            try
            {
                // Present
                var presentPositions = _readContext.GetPresentStreamPositions(presentStream);
                var numPresentValuesRead = ReadBooleanStream(presentStream, presentPositions, presentBuffer.AsSpan().Slice(0, maxValuesToRead));

                // Data
                var dataPostions = _readContext.GetTargetedStreamPositions(presentStream, dataStream);
                var numDataValuesRead = ReadNumericStream(dataStream, dataPostions, isSigned: true, dataBuffer.AsSpan().Slice(0, maxValuesToRead));

                var dataIndex = 0;
                var numValuesRead = 0;
                if (presentStreamRequired)
                {
                    for (int idx = 0; idx < numPresentValuesRead; idx++)
                    {
                        if (presentBuffer[idx])
                            outputValues[numValuesRead++] = dataBuffer[dataIndex++];
                        else
                            outputValues[numValuesRead++] = null;
                    }
                }
                else
                {
                    for (int idx = 0; idx < numDataValuesRead; idx++)
                        outputValues[numValuesRead++] = dataBuffer[idx];
                }

                return numValuesRead;
            }
            finally
            {
                ArrayPool<bool>.Shared.Return(presentBuffer);
                ArrayPool<long>.Shared.Return(dataBuffer);
            }
        }
    }
}
